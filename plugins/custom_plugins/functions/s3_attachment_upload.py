from airflow.models import Variable

from toloka.client import TolokaClient

import boto3
import botocore

OAUTH_TOKEN = Variable.get("OAUTH_TOKEN")
AWS_ACCESS_KEY_ID = Variable.get("aws_access_key_id")
AWS_SECRET_ACCESS_KEY = Variable.get("aws_secret_access_key")

toloka_client = TolokaClient(OAUTH_TOKEN, 'PRODUCTION')


def _get_s3_client(endpoint_url: str = 'https://storage.yandexcloud.net'):
    session = boto3.session.Session(
        aws_access_key_id = AWS_ACCESS_KEY_ID,
        aws_secret_access_key = AWS_SECRET_ACCESS_KEY,
    )
    return session.client(
        service_name='s3',
        endpoint_url=endpoint_url,
    )

def _host_attachment(
        s3: botocore.client.BaseClient,
        basedir: str,
        bucket_name: str,
        attachment_id: str,
) -> str:
    attachment = toloka_client.get_attachment(attachment_id)
    filepath = f'{basedir}/{attachment.name}'
    with open(filepath, 'wb') as att_out:
        toloka_client.download_attachment(attachment_id, att_out)
    s3.upload_file(f'{basedir}/{attachment.name}', bucket_name, attachment.name)
    return f'{s3.meta.endpoint_url}/{bucket_name}/{attachment.name}'
