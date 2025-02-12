from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import requests



def f_calculate_quality_measures():
    try:
        url = 'http://host.docker.internal:8084/func_calculate_quality_measures'
        # response = requests.get('http://localhost:8084/func_calculate_quality_measures')
        response = requests.get(url, timeout=10)
        response.raise_for_status()  # Raises an error for 4xx/5xx responses
        print("Response:", response.text)
    except requests.exceptions.RequestException as e:
        print(f"Error calling API: {e}")
        raise  # Ensure Airflow marks the task as failed

    #     return {"status": "error", "message": f"Failed to call func_calculate_quality_measures : {response.text}"}, response.status_code

# Define the Python function
def func_test():
    print("Hello from PythonOperator!")


# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    
    'email_on_retry': False,
    'retries': 0,
}

# Define the DAG
with DAG(
    'a_er_calculate_quality_measures_dag',
    default_args=default_args,
    description='A simple PythonOperator DAG to test ER',
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['entity resolution', 'quality measures'],

) as dag:

    func_test = PythonOperator(
        task_id='run_test_task',
        python_callable=func_test
    )

    fnc_calculate_quality_measures = PythonOperator(
        task_id='run_calculate_quality_measures_task',
        python_callable=f_calculate_quality_measures,  # Function to execute
    )

    func_test >> fnc_calculate_quality_measures  # This executes the task
