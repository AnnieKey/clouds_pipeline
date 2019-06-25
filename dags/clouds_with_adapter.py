from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from modules import adapter_version

#--------------------------------------------------------------------------------------
#Definition of DAG
#--------------------------------------------------------------------------------------

default_args = {
    "owner": "airflow",
    "start_date": datetime(2019, 1, 1)
}
dag = DAG("clouds_with_adapter", default_args=default_args, schedule_interval=timedelta(days=1))

#--------------------------------------------------------------------------------------
#Tasks
#--------------------------------------------------------------------------------------

task_create_file = PythonOperator(
    task_id = "create_file",
    provide_context = True,
    python_callable = adapter_version.Client.create_file,
    dag = dag
)

task_upload_file = PythonOperator(
    task_id = "upload_file",
    python_callable = adapter_version.Client.upload_file,
    dag = dag
)

task_download_file = PythonOperator(
    task_id = "download_file",
    python_callable = adapter_version.Client.download_file,
    dag = dag
)

task_wrap_data = PythonOperator(
    task_id = "wrap_data",
    provide_context = True,
    python_callable = adapter_version.Client.wrap_data,
    dag = dag
)

task_rename_file = PythonOperator(
    task_id = "rename_file",
    provide_context = True,
    python_callable = adapter_version.Client.rename_file,
    dag = dag
)

task_move_file = PythonOperator(
    task_id = "move_file",
    provide_context = True,
    python_callable = adapter_version.Client.move_file,
    dag = dag
)

#--------------------------------------------------------------------------------------
#Sequence of execution of tasks
#--------------------------------------------------------------------------------------

task_create_file.set_upstream(task_upload_file)
task_download_file.set_upstream(task_upload_file)
task_wrap_data.set_upstream(task_download_file)
task_rename_file.set_upstream(task_download_file)
task_move_file.set_upstream(task_rename_file)
