from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from modules import aws_cloud
from modules import google_cloud
from modules import azure_cloud

#---------------------------------------------------------------------------------------
#choosing cloud
#--------------------------------------------------------------------------------------

cloud=azure_cloud
bucket_name = 'testcloudanniekey'
bucket_for_movement='testmovement'

#------------------------------------------------------------------------------------
#declaration of variables (name of files, local and in cloud)
#--------------------------------------------------------------------------------------

file_name_local = "files/created_file.txt"
file_name_in_cloud = 'created_file.txt'
new_file_name_in_cloud = 'renamed_file.txt'
downloaded_file_name = 'files/downloaded_file.txt'

#-------------------------------------------------------------------------------------
#definition of DAG
#--------------------------------------------------------------------------------------

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2019, 1, 1)
}
dag = DAG('clouds', default_args=default_args, schedule_interval=timedelta(days=1))

#---------------------------------------------------------------------------------
#Tasks
#-------------------------------------------------------------------------------

task_create_file = PythonOperator (
    task_id='create_file',
    provide_context=True,
    python_callable=cloud.create_file,
    op_kwargs={
        'file_name': file_name_local,
    },
    dag=dag,
)

task_upload_file = PythonOperator(
    task_id='upload_file',
    python_callable=cloud.upload_file,
    op_kwargs={
        'file_name': file_name_local,
        'bucket_name': bucket_name,
        'file_name_in_cloud': file_name_in_cloud,
    },
    dag=dag
)


task_download_file = PythonOperator(
    task_id='download_file',
    python_callable=cloud.download_file,
    op_kwargs={
        'bucket_name': bucket_name,
        'file_name_in_cloud': file_name_in_cloud,
        'downloaded_file_name': downloaded_file_name,
    },
    dag=dag
)


task_wrap_data = PythonOperator (
    task_id='wrap_data',
    provide_context=True,
    python_callable=cloud.wrap_data,
    op_kwargs={
        'file_name': downloaded_file_name,
    },
    dag=dag,
)

task_rename_file = PythonOperator (
    task_id='rename_file',
    provide_context=True,
    python_callable=cloud.rename_file,
    op_kwargs={
        'bucket_name': bucket_name,
        'file_name': file_name_in_cloud,
        'new_file_name': new_file_name_in_cloud,
    },
    dag=dag,
)


task_move_file = PythonOperator (
    task_id='move_file',
    provide_context=True,
    python_callable=cloud.move_file,
    op_kwargs={
        'bucket_name': bucket_name,
        'file_name': new_file_name_in_cloud,
        'new_bucket_name': bucket_for_movement,
    },
    dag=dag,
)

task_create_file.set_upstream(task_upload_file)
task_download_file.set_upstream(task_upload_file)
task_wrap_data.set_upstream(task_download_file)
task_rename_file.set_upstream(task_download_file)
task_move_file.set_upstream(task_rename_file)