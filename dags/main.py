from files_to_rds import files_to_rds
from ibge_api import ibge_api
from extract_mongodb import extract_mongodb
from data_to_df import mongo_to_df,ibge_to_df
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

# Default Arguments
default_args = {
    'owner': 'Alexandre LM',
    'depends_on_past': False,
    'start_date' : datetime.now(),
    'retries' : 1,
    'retry_delay' : timedelta(minutes=1)
}

# Defining the DAG - Flow

dag = DAG(
    'ETL_AWS_Airflow_Pipeline',
    description='Pipeline since the extraction of the data from an API and a MongoDB uploading into a Postgres RDS in cloud',
    default_args=default_args
)

start_task = BashOperator(
    task_id='Start_task',
    bash_command='echo "Task has started"',
    dag=dag
)

requirements_task = BashOperator(
    task_id='Install_Requirements_task',
    bash_command='pip install --user -r /opt/airflow/data/requirements.txt',
    dag=dag
)

get_ibge_data_task = PythonOperator(
    task_id='get_ibge_data_task',
    python_callable = ibge_api,
    dag=dag
)

get_mongo_data_task = PythonOperator(
    task_id='get_mongo_data_task',
    python_callable = extract_mongodb,
    dag=dag
)

ibge_to_df = PythonOperator(
    task_id='ibge_to_df',
    python_callable = ibge_to_df,
    dag=dag
)

mongo_to_df = PythonOperator(
    task_id='mongo_to_df',
    python_callable = mongo_to_df,
    dag=dag
)

mongo_to_rds = PythonOperator(
    task_id='mongo_to_rds',
    python_callable = lambda: files_to_rds('mongo','mongo_data_rds'),
    dag=dag
)

ibge_to_rds = PythonOperator(
    task_id='ibge_to_rds',
    python_callable = lambda: files_to_rds('ibge','ibge_data_rds'),
    dag=dag
)

end_task = BashOperator(
    task_id='End_task',
    bash_command='echo "Task finished succesfully"',
    dag=dag
)

start_task >> requirements_task >> [get_ibge_data_task, get_mongo_data_task]
ibge_to_df.set_upstream(get_ibge_data_task)
mongo_to_df.set_upstream(get_mongo_data_task)
ibge_to_rds.set_upstream(ibge_to_df)
mongo_to_rds.set_upstream(mongo_to_df)
end_task.set_upstream([ibge_to_rds,mongo_to_rds])



