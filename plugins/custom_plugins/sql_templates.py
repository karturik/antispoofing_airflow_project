"""
SQL templates and queries for processing
"""

DB_AIRFLOW = '''
    host=rc1a-sgxb6yanrh3pkjcs.mdb.yandexcloud.net
    port=6432
    sslmode=require
    dbname=airflow_project_db
    user=airflow_project
    password=airflow_password
    target_session_attrs=read-write
'''

GET_ALL_SETS_COUNT = ''' SELECT count(*) FROM public.default_table; '''

CREATED_DET_TASK_INSERT = ''' INSERT INTO public.default_table (det_task_id, status, date)
                            VALUES (%s, 'CREATED_DET_TASK', %s); '''

SELECT_DET_UNFINISHED_TASKS = ''' SELECT det_task_id FROM public.defaul_table WHERE status = 'CREATED_DET_TASK'; '''

SAVE_DET_ANSWERS = ''' UPDATE public.default_table SET det_assignment_id = %s, det_data_phone = %s, det_data_video = %s, 
                        det_data_photo = %s, det_user_id = %s, status = 'GET_DET_ANSWERS' WHERE det_task_id = %s; '''

SAVE_VAL_TASK = ''' UPDATE public.default_table SET val_task_id = %s, status = 'CREATED_VAL_TASK' WHERE det_assignment_id = %s; '''

GET_UNFINISHED_VAL_TASKS = ''' SELECT val_task_id FROM public.defaul_table WHERE status = 'CREATED_VAL_TASK'; '''

CHANGE_STATUS = ''' UPDATE public.default_table SET status = %s WHERE det_assignment_id = %s; '''

DELETE_SET = ''' DELETE FROM public.default_table WHERE det_assignment_id = %s; '''

GET_REJECTED_ASSIGNMENTS = ''' SELECT det_data_video, det_data_photo, det_assignment FROM public.default_table WHERE status = 'CREATED_VAL_TASK'; '''