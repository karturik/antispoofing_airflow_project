"""
first dag, check, how much new sets needed and create new tasks to collect
"""
from . import (
    datetime,
    closing,
    dag,
    task,
    TolokaClient,
    TolokaTask,
    psycopg2,

    DB_AIRFLOW,
    GET_ALL_SETS_COUNT,
    CREATED_DET_TASK_INSERT,
    REQUEST_COUNT,
    DETECTION_POOL_ID,

    default_args,

    date
)


@dag(dag_id='create_det_tasks v1.0',
     default_args=default_args,
     start_date=datetime(2023, 3, 17),
     schedule_interval='@daily')
def create_det_tasks_main_flow():
    @task()
    def get_all_sets_count() -> int:
        with closing(psycopg2.connect(DB_AIRFLOW)) as conn:
            with closing(conn.cursor()) as cursor:
                cursor.execute(GET_ALL_SETS_COUNT)
                count = int(cursor.fetchone()[0])
        return count

    @task()
    def det_tasks_create(toloka_client: TolokaClient, existing_det_tasks_count: int) -> None:
        need_det_tasks_count = REQUEST_COUNT - existing_det_tasks_count
        created_tasks = []
        for _ in range(need_det_tasks_count):
            task_prepared = TolokaTask(
                input_values={'img_url': 'https://yastatic.net/s3/toloka/p/toloka-requester-page-project-preview/877a55555a5f199dff16.jpg'},
                pool_id=DETECTION_POOL_ID)
            res = toloka_client.create_task(task_prepared, allow_defaults=True, open_pool=True)
            created_tasks.append(res.id)

        with closing(psycopg2.connect(DB_AIRFLOW)) as conn:
            with closing(conn.cursor()) as cursor:
                for task_id in created_tasks:
                    cursor.execute(CREATED_DET_TASK_INSERT % (task_id, date))
                    conn.commit()

    total_sets_count = get_all_sets_count()
    if total_sets_count >= REQUEST_COUNT:
        pass
    else:
        det_tasks_create(total_sets_count)
