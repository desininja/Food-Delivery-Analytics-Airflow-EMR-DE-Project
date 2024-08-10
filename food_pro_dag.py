from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime

# Define your functions here

def push_s3_key(**context):
    s3_key = context['task_instance'].xcom_pull(task_ids='check_s3_for_file')
    context['task_instance'].xcom_push(key='s3_key', value=s3_key)
    print(f"Pushed S3 key to XCOM: {s3_key}")

def process_s3_key(**context):
    s3_key = context['task_instance'].xcom_pull(task_ids='push_s3_key', key='s3_key')
    print(f"Processing file: {s3_key}")
    return s3_key

def log_s3_keys(**context):
    s3_key = context['task_instance'].xcom_pull(task_ids='push_s3_key', key='s3_key')
    print(f"Logged S3 key: {s3_key}")

# DAG Configuration
S3_BUCKET = "food-delivery-project"
S3_KEY_PATTERN = "data-landing-zone/*.csv"
EMR_CLUSTER_ID = "j-2LELAN0V48PU3"
SPARK_SCRIPT_PATH = "s3://food-delivery-project/pyspark-scripts/pyspark_job.py"
SPARK_OUTPUT_PATH = "s3://food-delivery-project/output-files/"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': False,
    'retry_delay': timedelta(minutes=3)
}

dag = DAG(
    's3_to_emr_spark_with_xcoms_pro_version',
    default_args=default_args,
    description="DAG to trigger EMR Spark job based on S3 file arrival using XCOMS",
    start_date=datetime(2021, 1, 1),
    catchup=False,
)

check_s3_for_file = S3KeySensor(
    task_id='check_s3_for_file',
    bucket_name=S3_BUCKET,
    bucket_key=S3_KEY_PATTERN,
    wildcard_match=True,
    aws_conn_id='aws_default',
    timeout=18*60*60,
    poke_interval=60,
    dag=dag,
)

push_s3_key_task = PythonOperator(
    task_id='push_s3_key',
    python_callable=push_s3_key,
    provide_context=True,
    dag=dag,
)

log_s3_keys_task = PythonOperator(
    task_id='log_s3_keys',
    python_callable=log_s3_keys,
    provide_context=True,
    dag=dag,
)

process_s3 = PythonOperator(
    task_id='process_s3_key',
    python_callable=process_s3_key,
    provide_context=True,
    dag=dag,
)

step_adder = EmrAddStepsOperator(
    task_id='add_step',
    job_flow_id=EMR_CLUSTER_ID,
    aws_conn_id='aws_default',
    steps=[{
        'Name': 'Run PySpark Script',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode',
                'cluster',
                SPARK_SCRIPT_PATH,
                "{{ task_instance.xcom_pull(task_ids='process_s3_key', key='s3_key') }}",
            ],
        },
    }],
    dag=dag,
)

step_checker = EmrStepSensor(
    task_id='check_step',
    job_flow_id=EMR_CLUSTER_ID,
    step_id="{{ task_instance.xcom_pull(task_ids='add_step', key='return_value')[0] }}",
    aws_conn_id='aws_default',
    poke_interval=120,  # Check every 2 minutes
    timeout=86400,  # Fail if not completed in 1 day
    mode='poke',
    dag=dag,
)

check_s3_for_file >> push_s3_key_task >> log_s3_keys_task >> process_s3 >> step_adder >> step_checker
