from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
import boto3

# DAG Configuration
S3_BUCKET = "food-delivery-project"
S3_KEY_PATTERN = "data-landing-zone/*.csv"
S3_KEY = "data-landing-zone/"
EMR_CLUSTER_ID = "j-3TKI57YPJFDLK"
SPARK_SCRIPT_PATH = "s3://food-delivery-project/pyspark-scripts/pyspark_job.py"
SPARK_OUTPUT_PATH = "s3://food-delivery-project/output-files/"

def processing_func(**context):
    print("Getting the File from S3 Location")
    s3 = boto3.client('s3')
    object_key = None
    for obj in s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=S3_KEY)['Contents']:
        if 'Orders_data.csv' in obj['Key']:
            object_key = obj['Key']
            data_location_s3_uri = 's3://'+S3_BUCKET+'/'+object_key
            print(f"File to process: {data_location_s3_uri}")
            context['task_instance'].xcom_push(key='s3_key', value=data_location_s3_uri)
            print(f"Pushed S3 key to XCOM: {data_location_s3_uri}")

    return data_location_s3_uri

def pull_function(**context):
    s3_key = context['task_instance'].xcom_pull(task_ids='push_s3_key', key='s3_key')
    print(f"S3 object key from pull function: {s3_key}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': False,
    'retry_delay': timedelta(minutes=3)
}

dag = DAG(
    's3_to_emr_spark_with_xcoms',
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
    poke_interval=15,
    dag=dag,
)

push_s3_key_task = PythonOperator(
    task_id='push_s3_key',
    python_callable=processing_func,
    provide_context=True,
    dag=dag,
)

pull_task = PythonOperator(
    task_id='pull_task', 
    python_callable=pull_function,
    provide_context=True,
    dag=dag
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
                SPARK_SCRIPT_PATH, "{{ task_instance.xcom_pull(task_ids='push_s3_key', key='s3_key') }}"
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

check_s3_for_file >> push_s3_key_task >> pull_task >> step_adder >> step_checker
