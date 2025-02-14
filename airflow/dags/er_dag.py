from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
import requests

def func_file_extraction():
    try:
        url = 'http://host.docker.internal:8081/func_er_clustering'
        response = requests.get(url, timeout=10)
        response.raise_for_status()  # Raises an error for 4xx/5xx responses
        print("Response:", response.text)
    except requests.exceptions.RequestException as e:
        print(f"Error calling API: {e}")
        raise  # Ensure Airflow marks the task as failed

    #     return {"status": "error", "message": f"Failed to call func_calculate_quality_measures : {response.text}"}, response.status_code

def func_er_with_blocking():
    try:
        url = 'http://host.docker.internal:8082/func_er_with_blocking'
        response = requests.get(url, timeout=10)
        response.raise_for_status()  # Raises an error for 4xx/5xx responses
        print("Response:", response.text)
    except requests.exceptions.RequestException as e:
        print(f"Error calling API: {e}")
        raise  # Ensure Airflow marks the task as failed

    #     return {"status": "error", "message": f"Failed to call func_calculate_quality_measures : {response.text}"}, response.status_code

def func_er_with_brute_force():
    try:
        url = 'http://host.docker.internal:8083/func_er_with_brute_force'
        response = requests.get(url, timeout=300)
        response.raise_for_status()  # Raises an error for 4xx/5xx responses
        print("Response:", response.text)
    except requests.exceptions.RequestException as e:
        print(f"Error calling API: {e}")
        raise  # Ensure Airflow marks the task as failed

    #     return {"status": "error", "message": f"Failed to call func_calculate_quality_measures : {response.text}"}, response.status_code

def func_calculate_quality_measures():
    try:
        url = 'http://host.docker.internal:8084/func_calculate_quality_measures'
        response = requests.get(url, timeout=10)
        response.raise_for_status()  # Raises an error for 4xx/5xx responses
        print("Response:", response.text)
    except requests.exceptions.RequestException as e:
        print(f"Error calling API: {e}")
        raise  # Ensure Airflow marks the task as failed

    #     return {"status": "error", "message": f"Failed to call func_calculate_quality_measures : {response.text}"}, response.status_code

def func_er_clustering():
    try:
        url = 'http://host.docker.internal:8085/func_er_clustering'
        response = requests.get(url, timeout=10)
        response.raise_for_status()  # Raises an error for 4xx/5xx responses
        print("Response:", response.text)
    except requests.exceptions.RequestException as e:
        print(f"Error calling API: {e}")
        raise  # Ensure Airflow marks the task as failed

    #     return {"status": "error", "message": f"Failed to call func_calculate_quality_measures : {response.text}"}, response.status_code


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    'a_er_seq_dag',
    default_args=default_args,
    description='seq_dag for entity resolution',
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['entity resolution', 'python operator', 'sequential dag'],
) as dag:

    file_extraction = PythonOperator(
        task_id='task_file_extraction',
        python_callable=func_file_extraction,  # Function to execute
        )

    er_with_blocking = PythonOperator(
        task_id='task_er_with_blocking',
        python_callable=func_er_with_blocking,  # Function to execute
        )

    er_with_brute_force = PythonOperator(
        task_id='task_er_with_brute_force',
        python_callable=func_er_with_brute_force,  # Function to execute
        )

    calculate_quality_measures = PythonOperator(
        task_id='task_calculate_quality_measures',
        python_callable=func_calculate_quality_measures ,  # Function to execute
        )

    er_clustering = PythonOperator(
        task_id='task_er_clustering',
        python_callable=func_er_clustering,  # Function to execute
        )

    file_extraction >> er_with_blocking >> er_with_brute_force >> calculate_quality_measures >> er_clustering