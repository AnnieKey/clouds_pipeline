from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator


import boto3

#definition of client
s3 = boto3.client('s3')

#------------------------------------------------------------
#definition of functions
#--------------------------------------------------------------

def create_file_with_random_data(*args, **kwargs):
    import random
    tmp=[random.random() for _ in range(100)]
    file_name="random_data.txt"
    with open(file_name, 'w') as f:
        for i in range(len(tmp)):
            f.write(str(i) + ":")
            f.write(str(tmp[i]))
            f.write(' ')
    pass


def upload_file_to_S3(file_name, bucket_name, file_name_in_s3):
    s3.upload_file(file_name, bucket_name, file_name_in_s3)


def download_file_from_s3 (bucket_name, file_name_in_cloud, file_name_after_downloaded):
    s3.download_file(bucket_name, file_name_in_cloud, file_name_after_downloaded)


def wrap_data(file_name, *args, **kwargs):
    import json
    file =open(file_name, "r")
    data=file.read()
    data_json = json.dumps(data, separators=(":",""))
    return data_json


def rename_file(file_name, bucket_name, new_file_name, *args, **kwargs):
    s3.copy_object(Bucket=bucket_name, Key=new_file_name, CopySource=bucket_name+'/'+file_name)
    s3.delete_object(Bucket=bucket_name, Key=file_name)


def move_file(file_name, bucket_name, new_bucket_name, *args, **kwargs):
    s3.copy_object(Bucket=new_bucket_name, Key=file_name, CopySource=bucket_name+'/'+file_name)
    s3.delete_object(Bucket=bucket_name, Key=file_name)

#-----------------------------------------------------------------------------------
#definition of DAG
#--------------------------------------------------------------------------------------

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2019, 1, 1)
}
dag = DAG('aws_cloud', default_args=default_args, schedule_interval=timedelta(days=1))


#---------------------------------------------------------------------------------
#Tasks
#-------------------------------------------------------------------------------

task_create_file = PythonOperator (
    task_id='create_file',
    provide_context=True,
    python_callable=create_file_with_random_data,
    dag=dag,
)


upload_to_S3_task = PythonOperator(
    task_id='upload_to_s3',
    python_callable=upload_file_to_S3,
    op_kwargs={
        'file_name': '/home/anniekey/Projects/cloud_pipeline/files/random_data.txt',
        'bucket_name': 'testcloudanniekey',
        'file_name_in_s3': 'from_task.txt',
    },
    dag=dag
)


download_from_S3_task = PythonOperator(
    task_id='download_from_s3',
    python_callable=download_file_from_s3,
    op_kwargs={
        'bucket_name': 'testcloudanniekey',
        'file_name_in_cloud': 'from_task.txt',
        'file_name_after_downloaded': '/home/anniekey/Projects/cloud_pipeline/files/download_from_s3.txt',
    },
    dag=dag
)


wrap_data_task = PythonOperator (
    task_id='wrap_data',
    provide_context=True,
    python_callable=wrap_data,
    op_kwargs={'file_name': '/home/anniekey/Projects/cloud_pipeline/files/download_from_s3.txt'},
    dag=dag,
)

rename_file_task = PythonOperator (
    task_id='rename_file',
    provide_context=True,
    python_callable=rename_file,
    op_kwargs={
        'bucket_name': 'testcloudanniekey',
        'file_name': '/home/anniekey/Projects/cloud_pipeline/files/from_task.txt',
        'new_file_name': '/home/anniekey/Projects/cloud_pipeline/files/refactor_name.txt',
    },
    dag=dag,
)


task_move_file = PythonOperator (
    task_id='move_file',
    provide_context=True,
    python_callable=move_file,
    op_kwargs={
        'bucket_name': 'testcloudanniekey',
        'file_name': '/home/anniekey/Projects/cloud_pipeline/files/refactor_name.txt',
        'new_bucket_name': 'testmovement',
    },
    dag=dag,
)

task_create_file.set_upstream(upload_to_S3_task)
download_from_S3_task.set_upstream(upload_to_S3_task)
wrap_data_task.set_upstream(download_from_S3_task)
rename_file_task.set_upstream(download_from_S3_task)
task_move_file.set_upstream(rename_file_task)