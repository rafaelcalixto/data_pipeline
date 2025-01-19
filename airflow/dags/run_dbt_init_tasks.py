from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow_dbt.operators.dbt_operator import (
    DbtDepsOperator
)

# Defining default arguments
default_args = {
  'dir': '/opt/airflow/dbt',
  'start_date': days_ago(1),
  'dbt_bin': '/home/airflow/.local/bin/dbt'
}

# Defining the DAG
dag = DAG(
  dag_id='run_dbt_init_tasks', 
  default_args=default_args, 
  schedule_interval=None,
)

with dag:

  dbt_deps = DbtDepsOperator(
    task_id='dbt_deps',
  )

  generate_dbt_docs = BashOperator(
    task_id='generate_dbt_docs',
    bash_command='dbt docs generate --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt',
    dag=dag,
  )
  
dbt_deps >> generate_dbt_docs
