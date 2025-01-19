from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from pandas import read_csv
import os

# Defining default arguments
default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "retries": 1,
}

# Defining the DAG
dag = DAG(
    "transactions_data_ingestion",
    default_args=default_args,
    description="Ingest data from customer_transactions.csv into company_dw",
    schedule_interval=None,
)

def create_schema_if_not_exists(schema_name):
    hook = PostgresHook(postgres_conn_id="company_dw")
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
    conn.commit()
    cursor.close()
    conn.close()

def delete_existing_table(schema, table_name):
    hook = PostgresHook(postgres_conn_id="company_dw")
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {schema}.{table_name} CASCADE;")
    conn.commit()
    cursor.close()
    conn.close()

def create_table(file_path, schema, table_name):
    hook = PostgresHook(postgres_conn_id="company_dw")
    conn = hook.get_conn()
    cursor = conn.cursor()

    df = read_csv(file_path)
    df_columns = list(df.columns)
    columns = ",".join(df_columns)

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
        {", ".join([f"{col} TEXT" for col in df_columns])}
    );
    """
    
    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()
    conn.close()

def ingest_csv_to_postgres(file_path, table_name):
    hook = PostgresHook(postgres_conn_id="company_dw")
    conn = hook.get_conn()
    cursor = conn.cursor()

    with hook.get_conn() as connection:
        hook.copy_expert(
            f"COPY bronze.{table_name} FROM stdin WITH (FORMAT csv);",
            file_path)

    conn.commit()
    cursor.close()
    conn.close()

with dag:
    create_bronze_schema = PythonOperator(
        task_id="create_bronze_schema",
        python_callable=create_schema_if_not_exists,
        op_kwargs={"schema_name": "bronze"},
    )

    create_silver_schema = PythonOperator(
        task_id="create_silver_schema",
        python_callable=create_schema_if_not_exists,
        op_kwargs={"schema_name": "silver"},
    )

    create_gold_schema = PythonOperator(
        task_id="create_gold_schema",
        python_callable=create_schema_if_not_exists,
        op_kwargs={"schema_name": "gold"},
    )

    delete_bronze_table = PythonOperator(
        task_id="delete_bronze_table",
        python_callable=delete_existing_table,
        op_kwargs={
            "schema": "bronze",
            "table_name": "customer_transactions"
        },
    )

    delete_silver_table_dim = PythonOperator(
        task_id="delete_silver_table_dim",
        python_callable=delete_existing_table,
        op_kwargs={
            "schema": "silver",
            "table_name": "dim_table"
        },
    )

    delete_silver_table_fact = PythonOperator(
        task_id="delete_silver_table_fact",
        python_callable=delete_existing_table,
        op_kwargs={
            "schema": "silver",
            "table_name": "fact_table"
        },
    )

    create_bronze_table = PythonOperator(
        task_id="create_bronze_table",
        python_callable=create_table,
        op_kwargs={
            "file_path": "/opt/airflow/data/customer_transactions.csv", 
            "schema": "bronze",
            "table_name": "customer_transactions"
            },
    )

    create_silver_table_dim = PythonOperator(
        task_id="create_silver_table_dim",
        python_callable=create_table,
        op_kwargs={
            "file_path": "/opt/airflow/data/dim_table.csv", 
            "schema": "silver",
            "table_name": "dim_table"
            },
    )

    create_silver_table_fact = PythonOperator(
        task_id="create_silver_table_fact",
        python_callable=create_table,
        op_kwargs={
            "file_path": "/opt/airflow/data/fact_table.csv", 
            "schema": "silver",
            "table_name": "fact_table"
            },
    )

    ingest_transactions_data = PythonOperator(
        task_id="ingest_accounts",
        python_callable=ingest_csv_to_postgres,
        op_kwargs={
            "file_path": "/opt/airflow/data/customer_transactions.csv", 
            "table_name": "customer_transactions"
            },
    )

    create_bronze_schema.set_downstream(delete_bronze_table)
    delete_bronze_table.set_downstream(create_bronze_table)
    create_bronze_table.set_downstream(ingest_transactions_data)

    create_silver_schema >> [delete_silver_table_dim, delete_silver_table_fact]
    create_silver_table_dim.set_upstream(delete_silver_table_dim)
    create_silver_table_fact.set_upstream(delete_silver_table_fact)

    create_gold_schema