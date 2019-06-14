from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from google.cloud import storage

#------------------------------------------------------------
#definition of functions
#--------------------------------------------------------------

def upload_file_to_GS(bucket_name, source_file_name, destination_file_name):

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_file_name)
    blob.upload_from_filename(source_file_name)

    print('File {} uploaded to {}.'.format(
        source_file_name,
        destination_file_name))


def wrap_data(file_name, *args, **kwargs):
    import json
    file =open(file_name, "r")
    data=file.read()
    data_json = json.dumps(data, separators=(":",""))
    return data_json


def download_file(bucket_name, source_file_name, destination_file_name):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(source_file_name)

    blob.download_to_filename(destination_file_name)

    print('File {} downloaded to {}.'.format(
        source_file_name,
        destination_file_name))


def rename_file(bucket_name, file_name, new_name):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)

    new_blob = bucket.rename_blob(blob, new_name)

    print('File {} has been renamed to {}'.format(
        blob.name, new_blob.name))
#-----------------------------------------------------------------------------------
#definition of DAG
#--------------------------------------------------------------------------------------

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2019, 1, 1)
}
dag = DAG('google_cloud', default_args=default_args, schedule_interval=timedelta(days=1))

#---------------------------------------------------------------------------------
#Tasks
#-------------------------------------------------------------------------------

upload_to_GS_task = PythonOperator(
    task_id='upload_to_gs',
    python_callable=upload_file_to_GS,
    op_kwargs={
        'source_file_name': '/home/anniekey/Projects/cloud_pipeline/random_data.txt',
        'bucket_name': 'testclouds',
        'destination_file_name': 'from_task.txt',
    },
    dag=dag
)

download_from_GS_task = PythonOperator(
    task_id='download_from_gs',
    python_callable=download_file,
    op_kwargs={
        'source_file_name': 'random_data.txt',
        'bucket_name': 'testclouds',
        'destination_file_name': 'from_task.txt',
    },
    dag=dag
)

rename_file_task = PythonOperator(
    task_id='rename_file',
    python_callable=rename_file,
    op_kwargs={
        'source_file_name': 'random_data.txt',
        'bucket_name': 'testclouds',
        'destination_file_name': 'renamed_file.txt',
    },
    dag=dag
)



