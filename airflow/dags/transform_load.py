from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
  'owner': 'airflow'
}

with DAG(
  dag_id='transform_load',
  start_date=days_ago(1),
  schedule_interval='@daily',
  default_args=default_args,
  catchup=False,
) as dag:
  wait_data_extracted = ExternalTaskSensor(
    task_id='wait_data_loaded',
    external_dag_id='extract_data',
    allowed_states=['success'],
    execution_delta=timedelta(minutes=1)
  )
  populate_country = PostgresOperator(
    task_id='populate_country',
    postgres_conn_id='postgres_default',
    sql='sql/populate_country.sql',
    dag=dag
  )
  populate_customer = PostgresOperator(
    task_id='populate_customer',
    postgres_conn_id='postgres_default',
    sql='sql/populate_customer.sql',
    dag=dag
  ) 
  populate_product = PostgresOperator(
    task_id='populate_product',
    postgres_conn_id='postgres_default',
    sql='sql/populate_product.sql',
    dag=dag
  )
  populate_date = PostgresOperator(
    task_id='populate_date',
    postgres_conn_id='postgres_default',
    sql='sql/populate_date.sql',
    dag=dag,
    params={
      'date': days_ago(1).strftime("%Y-%m-%d")
      #'start_date_x': '2020-03-01',
      #'end_date_x': '2020-03-31'
    }
  )
  populate_fact_sales = PostgresOperator(
    task_id='populate_fact_sales',
    postgres_conn_id='postgres_default',
    sql='sql/populate_fact_sales.sql',
    dag=dag
  )
  wait_data_extracted >> populate_country >> populate_customer >> populate_product >> populate_date >> populate_fact_sales