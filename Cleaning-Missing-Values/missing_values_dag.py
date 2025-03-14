from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta
import logging
import pandas as pd

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 3, 1)
      
}

# Initialize  DAG
with DAG(
    'clean missing values dag',
    default_args=default_args,
    description='this Dag excutes the first task Cleaning-Missing-Values',
    schedule_interval='@daily',  # Adjust as per your needs
    tags=['data-transformation'],
    catchup=False
) as dag:

    # Extract Data from Source 
    def extract_data(**kwargs):
        
        df = pd.read_csv('../data/climate_data.csv')
        temp_file_path = "/tmp/extracted_data.csv"
        
        df.to_csv(temp_file_path, index=False)

        kwargs['ti'].xcom_push(key="data_path", value=temp_file_path)
        logging.info(f"Extracted data saved to {temp_file_path}")

    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        retries=3,
        retry_delay=timedelta(minutes=5),
        on_failure_callback=lambda context: logging.error("Extraction failed!"),
    )

    # Step 2: Clean Data 
    def clean_data(**kwargs):
        ti = kwargs['ti']
        extracted_data_path = ti.xcom_pull(task_ids='extract_data', key="data_path")
        extracted_data =  pd.read_csv(extracted_data_path)
        logging.info(f"Cleaning data: {extracted_data}")
        # For numerical columns like "temperature" or "humidity," fill missing values with the mean or median. 
        num_cols = extracted_data.select_dtypes(include='number').columns

        extracted_data[num_cols]  = extracted_data[num_cols].fillna( extracted_data[num_cols].mean())

        for col  in extracted_data.select_dtypes(include='object').columns:
            extracted_data[col] = extracted_data[col].fillna(extracted_data[col].value_counts().mode()[0])
        
        temp_cleaned_file_path = "/tmp/cleaned_data.csv"
        extracted_data.to_csv(temp_cleaned_file_path, index=False)

        ti.xcom_push(key="cleaned_data_path", value=temp_cleaned_file_path) 
        logging.info(f"Cleaned data saved to {temp_cleaned_file_path}")

    clean_task = PythonOperator(
        task_id='clean_data',
        python_callable=clean_data,
        retries=3,
        retry_delay=timedelta(minutes=5),
        on_failure_callback=lambda context: logging.error("Cleaning failed!"),
    )

 
    # Step 3: load data to dest
    def load_data(**kwargs):
        ti = kwargs['ti']
        cleaned_data_path = ti.xcom_pull(task_ids='clean_data', key="cleaned_data_path")

        snowflake_hook = SnowflakeHook(snowflake_conn_id='my_snowflake_conn')
        # put csv to the external stage
        snowsql_put = f"PUT file://{cleaned_data_path} @mis_project"
        snowflake_hook.run(snowsql_put )

        # copy the data into the designated table from stage
        
        copy_sql = f"""
        COPY INTO  miss_data.data.missing_values_cleaned
        FROM @mis_project/cleaned_data.csv
        FILE_FORMAT = (TYPE = CSV);
        """
        snowflake_hook.run(copy_sql)

        logging.info("Data loaded ")

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        retries=3,
        retry_delay=timedelta(minutes=5),
        on_failure_callback=lambda context: logging.error("Loading failed!"),
    )


    # task dependencies 
    extract_task >> clean_task  >> load_task
