from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from custom_operators.MySqlToPostgresOperator import MySqlToPostgresOperator
from airflow.utils.dates import days_ago
default_args = {
  'owner': 'airflow'
}
with DAG(
  dag_id='extract_data',
  start_date=days_ago(1),
  schedule_interval='@daily',
  default_args=default_args,
  catchup=False,
) as dag:
  clear_staging_tables = PostgresOperator(
    task_id='clear_staging_tables',
    postgres_conn_id='postgres_default',
    sql="""
        TRUNCATE staging_sales_all;
        TRUNCATE staging_country;
        TRUNCATE staging_customer;
        TRUNCATE staging_product;
        """,
    dag=dag
  )
  extract_country = MySqlToPostgresOperator(
    task_id='extract_country',
    sql='sql/export_country.sql',
    postgres_table='staging_country',
    postgres_conn_id='postgres_default',
    mysql_conn_id='mysql_local',
    params={}
  )
  extract_product = MySqlToPostgresOperator(
    task_id='extract_product',
    sql='sql/export_product.sql',
    postgres_table='staging_product',
    postgres_conn_id='postgres_default',
    mysql_conn_id='mysql_local',
    params={}
  )
  extract_customer = MySqlToPostgresOperator(
    task_id='extract_customer',
    sql='sql/export_customer.sql',
    postgres_table='staging_customer',
    postgres_conn_id='postgres_default',
    mysql_conn_id='mysql_local',
    params={}
  )
  extract_sales = MySqlToPostgresOperator(
    task_id='extract_sales',
    sql='sql/export_sales.sql',
    postgres_table='staging_sales_all',
    postgres_conn_id='postgres_default',
    mysql_conn_id='mysql_local',
    params={
      'date': days_ago(1).strftime("%Y-%m-%d")
      #'start_date_x': '2021-0-01',
      #'end_date_x': '2020-03-31'
    }
  )
  clear_staging_tables >> extract_country >> extract_product >> extract_customer >> extract_sales
