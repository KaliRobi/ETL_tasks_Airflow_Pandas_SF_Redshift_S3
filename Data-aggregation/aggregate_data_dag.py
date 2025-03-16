from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator
from datetime import datetime, timedelta
import pandas as pd
import logging


default_arguments = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(hours=12),
    'start_date': datetime(2025, 3, 1)
}

with DAG(
    'aggregate-data-from-csv',
    default_args=default_arguments,
    schedule='30 0 * * 1',
    tags=['aggregate-data', 'weather-department'],
    catchup=False,
    description='This dag executes some data aggregation check the task in tasks.txt'

) as agg_dag:

    def extract_data(**kwargs):
        extracted_data = pd.read_csv('../data/ocean_temperature_data.csv')
        extract_data_path = 'tmp/ocean_temperature_raw.csv'
        extract_data.to_csv(extract_data_path, index=False)
        kwargs['ti'].xcom_push(key='extracted_data', value=extract_data_path)
        logging.info('data extracted')

    def extract_data_source_b(**kwargs):
        extract_data = pd.read_json('../data/ocean_temperature_data.json')
        temp_data_path_b = '/tmp/ocean_temperature_data_temp.csv'
        extract_data.to_csv(temp_data_path_b, index=False)
        kwargs['ti'].xcom_push(key='extract_data_b', value=temp_data_path_b)

    extract_data_task = PythonOperator(
        task_id='extract_data_task',
        python_callable=extract_data,
        dag=agg_dag,
        on_failure_callback=lambda context: logging.error(
            'data extract failed, extract_data_task'),

    )

    extract_data_task_b = PythonOperator(
        task_id='extract_data_task_b',
        python_callable=extract_data_source_b,
        dag=agg_dag,
        on_failure_callback=lambda context: logging.error(
            'data extract failed, extract_data_task_b '),
    )

    def merge_raw_data(**kwargs):
        ti = kwargs['ti']
        raw_data_path = '/tmp/raw_data_path.csv'
        raw_1_path = ti.xcom_pull(
            task_id='extract_data_task',
            key='extracted_data')
        raw_2_path = ti.xcom_pull(
            task_id='extract_data_task_b',
            key='extract_data_b')

        raw_1 = pd.read_csv(raw_1_path)
        raw_2 = pd.read_csv(raw_2_path)
        merge_raw_data = pd.concat([raw_1, raw_2], axis=0).drop_duplicates()
        merge_raw_data.to_csv(raw_data_path)
        ti.xcom_push(key='merge_raw_data', value=raw_data_path)

    merge_raw_data = PythonOperator(
        task_id='merge_raw_data',
        python_callable=merge_raw_data,
        dag=agg_dag,
        on_failure_callback=lambda c: logging.error('merge_raw_data failed'),
    )

    def data_mart_a(**kwargs):
        ti = kwargs['ti']
        tmp_temperature_data_path = '/tmp/temp_temperature_data.csv'

        temperature_data_path = ti.xcom_pull(
            task_id='merge_raw_data', key='merge_raw_data')

        temperature_data = pd.read_csv(temperature_data_path)

        temperature_mean = temperature_data['temperature'].groupby(
            temperature_data['location']).mean()

        temperature_tershold_passed = temperature_mean[temperature_mean > 15]

        temperature_tershold_passed.to_csv(
            tmp_temperature_data_path, index=False)

        ti.xcom_push(key='data_mart_a', value=tmp_temperature_data_path)

    data_mart_a_task = PythonOperator(
        task_id='data_mart_a_task',
        python_callable=data_mart_a,
        dag=agg_dag,
        on_failure_callback=lambda c: logging.error('data_mart_a_task failed'),
    )

    def data_mart_b(**kwargs):
        ti = kwargs['ti']
        tmp_below_treshold_temp_data_path = '/tmp/below_treshold_temp_data.csv'
        below_treshold_temp_data_path = ti.xcom_pull(
            task_id='merge_raw_data', key='merge_raw_data')
        temperature_data = pd.read_csv(below_treshold_temp_data_path)
        temperature_variability = temperature_data.groupby('location')[
            'temperature'].std()
        below_treshold_regions = temperature_variability[temperature_variability < 8]
        below_treshold_regions.to_csv(
            tmp_below_treshold_temp_data_path, index=False)

        ti.xcom_push(key='data_mart_b', value=below_treshold_temp_data_path)

    data_mart_b_task = PythonOperator(
        task_id='data_mart_b_task',
        python_callable=data_mart_b,
        dag=agg_dag,
        on_failure_callback=lambda c: logging.error('data_mart_b_task failed'),

    )

    def data_mart_c(**kwargs):
        ti = kwargs['ti']
        tmp_temperature_data_path = '/tmp/average_temp_per_region_depth_data.csv'
        temperature_data_path = ti.xcom_pull(
            task_id='merge_raw_data', key='merge_raw_data')
        temperature_data = pd.read_csv(temperature_data_path)
        grouped_temp = temperature_data.groupby(['location', 'depth'])[
            'temperature'].mean()

        below_critical_locations = grouped_temp[grouped_temp > 15]
        below_critical_locations.to_csv(tmp_temperature_data_path, index=False)
        ti.xcom_push(key='data_mart_c', value=tmp_temperature_data_path)

    data_mart_c_task = PythonOperator(
        task_id='data_mart_c_task',
        python_callable=data_mart_c,
        dag=agg_dag,
        on_failure_callback=lambda c: logging.error('data_mart_c_task failed'),
    )

    def sink_marts(**kwargs):
        ti = kwargs['ti']
        s3_hook = S3Hook(aws_conn_id='aws_default')
        redshift_hook = RedshiftSQLOperator(
            task_id='load_to_redshift',
            redshift_conn_id='redshift_default',
            autocommit=True
        )

        # Upload and load each data mart
        for mart_key in ['data_mart_a', 'data_mart_b', 'data_mart_c']:
            mart_path = ti.xcom_pull(task_ids=f'{mart_key}_task', key=mart_key)
            s3_key = f'{mart_key}.csv'
            s3_bucket = ''

            # upload to S3
            s3_hook.load_file(
                filename=mart_path,
                key=s3_key,
                bucket_name=s3_bucket,
                replace=True
            )
           

            # load into Redshift
            copy_query = f"""
                COPY aggregated_data
                FROM 's3://{s3_bucket}/{s3_key}'
                IAM_ROLE 'arn:aws:iam::123456789012:role/etl-redshift-role'
                FORMAT AS CSV
                IGNOREHEADER 1
                DELIMITER ','
            """
            redshift_hook.sql = copy_query
            redshift_hook.execute(context=kwargs)
            logging.info(f"Loaded {mart_key} into Redshift")

    sink_marts_task = PythonOperator(
        task_id='sink_marts_task',
        python_callable=sink_marts,
        dag=agg_dag,
        on_failure_callback=lambda c: logging.error('sink_marts_task failed'),
    )


[extract_data_task_b, extract_data_task] >> merge_raw_data >> [
    data_mart_a_task, data_mart_b_task, data_mart_c_task] >> sink_marts
