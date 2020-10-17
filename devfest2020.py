from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.contrib.operators.mysql_to_gcs import MySqlToGoogleCloudStorageOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

default_args = {
    'owner': 'rendy',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['hi@rendyistyping.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

bucket_name = 'rendy-test' # change to your bucket name here
sales_filename = 'sales_export.json'
sales_schema_filename = 'sales_schema.json'

with DAG('devfest2020', schedule_interval=timedelta(days=1), default_args=default_args) as dag:
    extract = MySqlToGoogleCloudStorageOperator(
                task_id='extract',
                sql='SELECT * FROM test_db.sales_table st', # change to your mysql table
                bucket=bucket_name,
                filename=sales_filename,
                schema_filename=sales_schema_filename,
                mysql_conn_id='devfest2020', # change to your mysql connection id
            )

    load = GoogleCloudStorageToBigQueryOperator(
                task_id='load',
                destination_project_dataset_table='project.rendy_test.sales', #change to your bq
                bucket=bucket_name,
                source_objects=[sales_filename],
                schema_object=sales_schema_filename,
                write_disposition='WRITE_TRUNCATE',
                create_disposition='CREATE_IF_NEEDED',
                source_format='NEWLINE_DELIMITED_JSON'
            )

    transform = BigQueryOperator(
                task_id='transform',
                sql="SELECT * REPLACE(REGEXP_REPLACE(cellphone, '[^0-9]', '') AS cellphone) FROM 'project.rendy_test.sales",
                use_legacy_sql=False,
                destination_dataset_table='project.rendy_test.sales_clean' #change to your bq

            )

    extract >> load >> transform
