from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pkg_resources

def check_installed_packages(**kwargs):
    installed_packages = {pkg.key for pkg in pkg_resources.working_set}
    required_packages = {"apache-airflow-providers-amazon"}
    missing_packages = required_packages - installed_packages
    if missing_packages:
        print(f"Missing packages: {missing_packages}")
    else:
        print("All required packages are installed.")

with DAG(
    'check_packages_dag',
    start_date=datetime(2024, 8, 10),
    schedule_interval=None,
    catchup=False,
) as dag:

    check_packages_task = PythonOperator(
        task_id='check_installed_packages',
        python_callable=check_installed_packages,
    )
