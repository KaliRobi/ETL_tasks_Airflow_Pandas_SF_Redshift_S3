from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
from failure_notify import notify_personel
import pandas as  pd
import logging


default_arguments= {
    'owner' : 'airflow',
    'retries' : 1,
    'retry_delay' : timedelta(minutes=15),
    'start_date': datetime(2025,3,1)
}

# start up dag
with DAG(
    'convert-columns-in-csv',
    default_args=default_arguments,
    schedule='0 07 * * 1',
    tags=['data-transformation'],
    catchup=False,
    description='this Dag excutes the second task Convert-data-rearrange-columns'


) as cleaning_dag:

    def extract_data(**kwargs):
        ti = kwargs['ti']

        df = pd.read_csv('../data/file.py')
        temp_file_path = '/tmp/convert_columns_task.csv'

        df.to_csv(temp_file_path, index=False)

        ti.xcom_push(key='data_path', value=temp_file_path)


    data_extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        on_failure_callback=notify_personel,
        dag=cleaning_dag,
    )

    def transform_data(**kwargs):
        ti = kwargs['ti']

        climate_data_path = ti.xcom_pull(task_ids='extract_data',  key='data_path')

        climate_data = pd.read_csv(climate_data_path)
        logging.info(f"Cleaning data: {climate_data}")

        climate_data['time'] = pd.to_datetime(climate_data['time'])

        climate_data = climate_data.rename(columns={'time':'datetime'})

        climate_data.set_index('datetime', inplace=True)

        cleaned_climate_data_path = '/tmp/cleaned_climate_data.csv'
        climate_data.to_csv(cleaned_climate_data_path)

        ti.xcom_push(key='cleaned_climate_date', value=cleaned_climate_data_path)
    
    transform_data_task = PythonOperator(
        task_id='cleaned_climate_data',
        python_callable=transform_data,
        dag=cleaning_dag,
        on_failure_callback= lambda context: logging.error("Cleaning failed!"),
    )
    
    def load_data(**kwargs):
        ti = kwargs['ti']
        mart_data_path = ti.xcom_pull(task_id='cleaned_climate_data', key='cleaned_climate_date')

        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')

        file_to_stage = f'PUT file://{mart_data_path} @mis_project'

        hook.run(file_to_stage)

        copy_into = f"""
            COPY INTO climate_data.climate.missing_values_cleaned
            FROM @mis_project/leaned_climate_data.csv
            FILE_FORMAT = (TYPE = CSV) """

        hook.run(copy_into)

        logging.info('Data sucessfully loaded')

    load_data_task = PythonOperator(
        task_id = 'upload_csv_file',
        python_callable=load_data,
        dag=cleaning_dag,
        on_failure_callback=lambda context: logging.error("data not updated to the WH")

    )

#dependencies
data_extract_task  >> transform_data_task >>  load_data_task
