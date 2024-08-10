from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def print_installed_packages(**kwargs):
    import pkg_resources
    installed_packages = {pkg.key for pkg in pkg_resources.working_set}
    print(f"Installed packages: {installed_packages}")

with DAG(
    'debug_dag',
    start_date=datetime(2024, 8, 10),
    schedule_interval=None,
    catchup=False,
) as dag:

    debug_task = PythonOperator(
        task_id='debug_task',
        python_callable=print_installed_packages,
    )
