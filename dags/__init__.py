__all__ = [
    'datetime',
    'timedelta',
    'closing',
    'dag',
    'task',
    'Variable',
    'TolokaClient',
    'TolokaTask',
    'DoesNotExistApiError',
    'ValidationApiError',
    'MajorityVote',
    'psycopg2',
    'pd',
    'boto3',
    'botocore',

    'DB_AIRFLOW',
    'GET_ALL_SETS_COUNT',
    'CREATED_DET_TASK_INSERT',
    'SELECT_DET_UNFINISHED_TASKS',
    'SAVE_DET_ANSWERS',
    'SAVE_VAL_TASK',
    'GET_UNFINISHED_VAL_TASKS',
    'CHANGE_STATUS',
    'DELETE_SET',
    'GET_REJECTED_ASSIGNMENTS',

    'Answer',
    '_get_s3_client',
    '_host_attachment',

    'OAUTH_TOKEN',
    'REQUEST_COUNT',
    'DETECTION_POOL_ID',
    'VALIDATION_POOL_ID',
    'AWS_ACCESS_KEY_ID',
    'AWS_SECRET_ACCESS_KEY',

    'toloka_client',
    'default_args',

    'date'
]

from datetime import datetime, timedelta
from contextlib import closing

from airflow.decorators import dag, task
from airflow.models import Variable

from toloka.client import TolokaClient, Task as TolokaTask
from toloka.client.exceptions import DoesNotExistApiError, ValidationApiError
from crowdkit.aggregation import MajorityVote

import psycopg2
import pandas as pd
import boto3
import botocore

from plugins.custom_plugins.sql_templates import (
    DB_AIRFLOW,
    GET_ALL_SETS_COUNT,
    CREATED_DET_TASK_INSERT,
    SELECT_DET_UNFINISHED_TASKS,
    SAVE_DET_ANSWERS,
    SAVE_VAL_TASK,
    GET_UNFINISHED_VAL_TASKS,
    CHANGE_STATUS,
    DELETE_SET,
    GET_REJECTED_ASSIGNMENTS
)

from plugins.custom_plugins.functions.answer import (
    Answer,
)

from plugins.custom_plugins.functions import (
    _get_s3_client,
    _host_attachment
)

OAUTH_TOKEN = Variable.get("OAUTH_TOKEN")
REQUEST_COUNT = int(Variable.get("request_count"))
DETECTION_POOL_ID = Variable.get("detection_pool_id")
VALIDATION_POOL_ID = Variable.get("validation_pool_id")
AWS_ACCESS_KEY_ID = Variable.get("aws_access_key_id")
AWS_SECRET_ACCESS_KEY = Variable.get("aws_secret_access_key")

toloka_client = TolokaClient(OAUTH_TOKEN, 'PRODUCTION')

default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

e = datetime.now()
date = '%s.%s.%s' % (e.day, e.month, e.year)