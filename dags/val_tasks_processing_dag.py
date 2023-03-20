"""
third dag, check validation tasks for answers
aggregate all answers
accept or reject sets
"""
from . import (
    datetime,
    closing,
    dag,
    task,
    ValidationApiError,
    MajorityVote,
    psycopg2,
    pd,

    DB_AIRFLOW,
    SELECT_DET_UNFINISHED_TASKS,
    CHANGE_STATUS,
    DELETE_SET,
    GET_REJECTED_ASSIGNMENTS,

    Answer,

    toloka_client,
    default_args,
)

from . import (
    _get_s3_client,
)


@dag(dag_id='val_tasks_processing v1.0',
     default_args=default_args,
     start_date=datetime(2023, 3, 17),
     schedule_interval='@daily')
def check_val_tasks_status_main_flow():

    @task
    def get_undoned_val_tasks_from_db() -> pd.DataFrame:
        with closing(psycopg2.connect(DB_AIRFLOW)) as conn:
            with closing(conn.cursor()) as cursor:
                cursor.execute(SELECT_DET_UNFINISHED_TASKS)
                conn.commit()
        val_tasks_to_check_status = pd.DataFrame(conn.fetchall(), columns=['val_task_id'])
        return val_tasks_to_check_status

    @task
    def check_val_tasks_status(val_tasks_to_check_status: pd.DataFrame) -> list[Answer]:
        result = []
        for val_task_id in val_tasks_to_check_status['val_task_id'].unique():
            try:
                val_task_assignments = list(toloka_client.get_assignments(task_id=val_task_id, status='ACCEPTED'))
                if len(val_task_assignments) >= 3:
                    for assignment in val_task_assignments:
                        for task, solution in zip(assignment.tasks, assignment.solutions):
                            if solution is None:
                                continue
                            result.append(Answer(
                                det_task_id=assignment.tasks[0].input_values['assignment_id'],
                                assignment_id=assignment.id,
                                user_id=assignment.user_id,
                                output_values=solution.output_values,
                            ))
            except ValidationApiError:
                pass
        return result

    @task
    def aggregate_val_tasks_answers(val_tasks_to_check_status: list[Answer]) -> tuple[list, list]:
        tuples = [
            (answer.det_task_id, answer.output_values['is_ok'], answer.user_id)
            for answer in val_tasks_to_check_status
        ]
        df = pd.DataFrame(tuples, columns=['task', 'label', 'worker'])
        aggregated = MajorityVote().fit_predict(df).to_dict()
        sets_to_accept = [assignment_id for assignment_id, is_ok in aggregated.items() if is_ok]
        sets_to_reject = [assignment_id for assignment_id, is_ok in aggregated.items() if not is_ok]
        return sets_to_accept, sets_to_reject

    @task
    def accept_assignments(sets_to_accept):
        with closing(psycopg2.connect(DB_AIRFLOW)) as conn:
            with closing(conn.cursor()) as cursor:
                for assignment_id in sets_to_accept:
                    toloka_client.accept_assignment(assignment_id=assignment_id, public_comment='Good work, thank you!')
                    cursor.execute(CHANGE_STATUS % ('ACCEPTED', assignment_id))
                    conn.commit()

    @task
    def reject_assignments(sets_to_reject):
        with closing(psycopg2.connect(DB_AIRFLOW)) as conn:
            with closing(conn.cursor()) as cursor:
                for assignment_id in sets_to_reject:
                    toloka_client.reject_assignment(assignment_id=assignment_id, public_comment='Answer has some errors!')
                    # cursor.execute(CHANGE_STATUS % ('REJECTED', assignment_id))
                    cursor.execute(DELETE_SET % (assignment_id))
                    conn.commit()

    @task
    def delete_rejected_sets_media_from_s3(sets_to_reject):
        s3 = _get_s3_client()
        with closing(psycopg2.connect(DB_AIRFLOW)) as conn:
            with closing(conn.cursor()) as cursor:
                cursor.execute(GET_REJECTED_ASSIGNMENTS)
                conn.commit()
        val_tasks_to_delete_media_data = pd.DataFrame(conn.fetchall(), columns=['video, photo, det_assignment_id'])
        df_for_deleting = val_tasks_to_delete_media_data.loc[val_tasks_to_delete_media_data['det_assignment_id'].isin(sets_to_reject)]
        response_photo_deleting = s3.delete_objects(
            Bucket='prefect-example-fin',
            Delete={"Objects": [{"Key": photo} for photo in df_for_deleting['photo'].unique()]},
        )
        response_video_deleting = s3.delete_objects(
            Bucket='prefect-example-fin',
            Delete={"Objects": [{"Key": video} for video in df_for_deleting['video'].unique()]},
        )

    val_tasks_to_check_status = get_undoned_val_tasks_from_db()
    if val_tasks_to_check_status:
        val_tasks_answers = check_val_tasks_status(val_tasks_to_check_status)
        sets_to_accept, sets_to_reject = aggregate_val_tasks_answers(val_tasks_answers)
        if sets_to_accept:
            accept_assignments(sets_to_accept)
        if sets_to_reject:
            delete_rejected_sets_media_from_s3(sets_to_reject)
            reject_assignments(sets_to_reject)
