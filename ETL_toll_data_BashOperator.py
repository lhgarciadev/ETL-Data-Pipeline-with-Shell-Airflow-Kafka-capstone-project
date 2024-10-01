from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

BASE_PATH = '/home/project/airflow/dags/finalassignment'
STAGING_PATH = "/home/project/airflow/dags/finalassignment/staging"

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

unzip_data = BashOperator(
    task_id = 'unzip_data',
    bash_command = f'tar -xzf {BASE_PATH}/tolldata.tgz -C {BASE_PATH}',
    dag = dag 
)

extract_data_from_csv = BashOperator(
    task_id = 'extract_data_from_csv',
    bash_command = f'cut -d"," -f1-4 {BASE_PATH}/vehicle-data.csv > {STAGING_PATH}/csv_data.csv',
    dag = dag,
)

extract_data_from_tsv = BashOperator(
    task_id = 'extract_data_from_tsv',
    bash_command = f'cut -f5-7 {BASE_PATH}/tollplaza-data.tsv > {STAGING_PATH}/tsv_data.csv',
    dag = dag,
)

extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command=f'awk \'NF{{print $(NF-1),$NF}}\' OFS="\\t" {BASE_PATH}/payment-data.txt > {STAGING_PATH}/fixed_width_data.csv',
    dag=dag,
)

consolidate_data = BashOperator(
    task_id = 'consolidate_data',
    bash_command = f'paste {STAGING_PATH}/csv_data.csv {STAGING_PATH}/tsv_data.csv {STAGING_PATH}/fixed_width_data.csv > {STAGING_PATH}/extracted_data.csv',
    dag = dag,
)

transform_data = BashOperator(
    task_id = 'transform_data',
    bash_command = f'awk \'{{$5 = toupper($5)}}1\' OFS="," {STAGING_PATH}/extracted_data.csv > {STAGING_PATH}/transformed_data.csv',
    dag = dag,
)

unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data
