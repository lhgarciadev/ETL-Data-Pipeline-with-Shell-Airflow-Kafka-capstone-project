import os
from datetime import timedelta
import requests
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

STAGING_PATH = '/home/project/airflow/dags/python_etl/staging'

default_args = {
    'owner': 'Leo_dev',
    'start_date': days_ago(0),
    'email': ['leo_dev@egmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),
)

def download_dataset():
    url = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz"
    response = requests.get(url)
    if response.status_code == 200:
        os.makedirs(STAGING_PATH, exist_ok=True)
        with open(f"{STAGING_PATH}/tolldata.tgz", "wb") as f:
            f.write(response.content)
        print("Dataset downloaded successfully.")
    else:
        raise Exception("Failed to download the dataset.")

def unzip_data():
    os.system(f'tar -xzf {STAGING_PATH}/tolldata.tgz -C {STAGING_PATH}')

def extract_data_from_csv():
    df = pd.read_csv(f'{STAGING_PATH}/vehicle-data.csv', header=None)
    df.iloc[:, :4].to_csv(f'{STAGING_PATH}/csv_data.csv', index=False, header=False)

def extract_data_from_tsv():
    df = pd.read_csv(f'{STAGING_PATH}/tollplaza-data.tsv', sep='\t', header=None)
    df.iloc[:, 4:7].to_csv(f'{STAGING_PATH}/tsv_data.csv', index=False, header=False)

def extract_data_from_fixed_width():
    df = pd.read_fwf(f'{STAGING_PATH}/payment-data.txt', header=None)
    df.iloc[:, -2:].to_csv(f'{STAGING_PATH}/fixed_width_data.csv', index=False, header=False, sep='\t')

def consolidate_data():
    csv_data = pd.read_csv(f'{STAGING_PATH}/csv_data.csv', header=None)
    tsv_data = pd.read_csv(f'{STAGING_PATH}/tsv_data.csv', header=None)
    fixed_width_data = pd.read_csv(f'{STAGING_PATH}/fixed_width_data.csv', sep='\t', header=None)
    
    consolidated = pd.concat([csv_data, tsv_data, fixed_width_data], axis=1)
    consolidated.to_csv(f'{STAGING_PATH}/extracted_data.csv', index=False, header=False)

def transform_data():
    df = pd.read_csv(f'{STAGING_PATH}/extracted_data.csv', header=None)
    df.iloc[:, 4] = df.iloc[:, 4].str.upper()
    df.to_csv(f'{STAGING_PATH}/transformed_data.csv', index=False, header=False)

download_dataset = PythonOperator(
    task_id='download_dataset',
    python_callable=download_dataset,
    dag=dag,
)

unzip_data = PythonOperator(
    task_id='unzip_data',
    python_callable=unzip_data,
    dag=dag,
)

extract_data_from_csv = PythonOperator(
    task_id='extract_data_from_csv',
    python_callable=extract_data_from_csv,
    dag=dag,
)

extract_data_from_tsv = PythonOperator(
    task_id='extract_data_from_tsv',
    python_callable=extract_data_from_tsv,
    dag=dag,
)

extract_data_from_fixed_width = PythonOperator(
    task_id='extract_data_from_fixed_width',
    python_callable=extract_data_from_fixed_width,
    dag=dag,
)

consolidate_data = PythonOperator(
    task_id='consolidate_data',
    python_callable=consolidate_data,
    dag=dag,
)

transform_data = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

download_dataset >> unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data
