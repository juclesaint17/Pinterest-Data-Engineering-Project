from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator, DatabricksRunNowOperator
from datetime import datetime, timedelta 


notebook_task = {
    'notebook_path': '/Workspace/Users/jngouedi@gmail.com/read_s3_bucket_from_databricks',
}


#Define params for Run Now Operator
notebook_params = {
    "Variable":5
}


default_args = {
    'owner': '0e7ae8feb921',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}


with DAG(dag_id='0e7ae8feb921_dag',
    start_date=datetime(2024,8,24),
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args
    ) as dag:


    opr_submit_run = DatabricksSubmitRunOperator(
        task_id='submit_run',
        databricks_conn_id='databricks_default',
        existing_cluster_id="1108-162752-8okw8dgg",
        notebook_task=notebook_task
    )
    opr_submit_run

