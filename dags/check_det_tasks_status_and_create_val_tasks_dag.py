"""
second dag, collect assignments from finished
detection tasks and create new validation tasks
"""
from . import (
    datetime,
    closing,
    dag,
    task,
    TolokaTask,
    DoesNotExistApiError,
    psycopg2,
    pd,

    DB_AIRFLOW,
    SELECT_DET_UNFINISHED_TASKS,
    SAVE_DET_ANSWERS,
    SAVE_VAL_TASK,

    Answer,

    VALIDATION_POOL_ID,

    toloka_client,
    default_args,
)

from . import (
    _get_s3_client,
    _host_attachment
)


@dag(dag_id='check_det_tasks_status_and_create_val_tasks v1.0',
     default_args=default_args,
     start_date=datetime(2023, 3, 17),
     schedule_interval='@daily')
def det_tasks_processing_and_create_val_tasks_main_flow():
    @task()
    def get_created_det_tasks_from_db() -> pd.DataFrame:
        with closing(psycopg2.connect(DB_AIRFLOW)) as conn:
            with closing(conn.cursor()) as cursor:
                cursor.execute(SELECT_DET_UNFINISHED_TASKS)
                conn.commit()
        det_tasks = pd.DataFrame(conn.fetchall(), columns=['det_task_id'])
        return det_tasks

    @task
    def get_det_tasks_assignment() -> list[Answer]:
        result = []
        for det_task_id in det_tasks_df['det_task_id']:
            try:
                assignments = list(toloka_client.get_assignments(task_id=det_task_id, status=['SUBMITTED']))
                for assignment in assignments:
                    for task, solution in zip(assignment.tasks, assignment.solutions):
                        if solution is None:
                            continue
                        result.append(Answer(
                            det_task_id=task.id,
                            assignment_id=assignment.id,
                            user_id=assignment.user_id,
                            output_values=solution.output_values,
                        ))
            except DoesNotExistApiError:
                # у таски еще нет подходящих ассайнментов
                pass
        return result

    @task
    def db_save_answers(answers: list[Answer]) -> list[tuple]:
        query = SAVE_DET_ANSWERS
        data = [
            (
                answer.assignment_id,
                answer.output_values['phone_'],
                answer.output_values['video_'],
                answer.output_values['photo_'],
                answer.user_id,
                answer.det_task_id,
            )
            for answer in answers
        ]
        with closing(psycopg2.connect(DB_AIRFLOW)) as conn:
            with closing(conn.cursor()) as cursor:
                cursor.executemany(query, data)
                conn.commit()
        return data

    @task
    def s3_det_answers_upload(answers: list[tuple]) -> list[Answer]:
        result = []
        s3 = _get_s3_client()
        basedir = 'attachments'
        bucket_name = 'prefect-example-fin'
        for assignment_id, phone_attachment_model, \
                video_attachment_id, photo_attachment_id, \
                user_id, task_id in answers:
            video_url = _host_attachment(s3, basedir, bucket_name, video_attachment_id)
            photo_url = _host_attachment(s3, basedir, bucket_name, photo_attachment_id)
            ans = Answer(
                det_task_id=task_id,
                assignment_id=assignment_id,
                user_id=user_id,
                output_values={
                    'video_url_': video_url,
                    'photo_url_': photo_url,
                    'phone_': phone_attachment_model
                },
            )
            result.append(ans)
        return result

    @task
    def create_val_tasks(answers: list[Answer]) -> list[TolokaTask]:
        val_pool_id = VALIDATION_POOL_ID
        tasks_prepared = [
            TolokaTask(
                pool_id=val_pool_id,
                unavailable_for=[answer.user_id],
                input_values={
                    'video_url': answer.output_values['video_url_'],
                    'photo_url': answer.output_values['photo_url_'],
                    'phone_': answer.output_values['phone_'],
                    'assignment_id': answer.assignment_id,
                })
            for answer in answers
        ]
        res = toloka_client.create_tasks(tasks_prepared, allow_defaults=True, open_pool=True)
        return list(res.items.values())

    @task
    def save_val_task_id(created_val_tasks):
        query = SAVE_VAL_TASK
        for task in created_val_tasks:
            with closing(psycopg2.connect(DB_AIRFLOW)) as conn:
                with closing(conn.cursor()) as cursor:
                    cursor.execute(query % (task.id, task.input_values['assignment_id']))
                    conn.commit()

    det_tasks_df = get_created_det_tasks_from_db()
    det_answers = get_det_tasks_assignment()

    if det_answers:
        answers_data = db_save_answers(det_answers)
        det_data_for_val_tasks = s3_det_answers_upload(answers_data)
        created_val_tasks = create_val_tasks(answers=det_data_for_val_tasks)
        save_val_task_id(created_val_tasks)
