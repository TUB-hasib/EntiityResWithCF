from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.dates import days_ago



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
    tags=['entity resolution'],
) as dag:

    func_file_extraction = SimpleHttpOperator(
        task_id='func_file_extraction',
        http_conn_id='http_default',
        endpoint='func_file_extraction',
        method='GET',
        response_check=lambda response: response.status_code == 200,
        dag=dag,
    )

    func_er_with_blocking = SimpleHttpOperator(
        task_id='func_er_with_blocking',
        http_conn_id='http_default',
        endpoint='func_er_with_blocking',
        method='GET',
        response_check=lambda response: response.status_code == 200,
        dag=dag,
    )

    func_er_with_brute_force = SimpleHttpOperator(
        task_id='func_er_with_brute_force',
        http_conn_id='http_default',
        endpoint='func_er_with_brute_force',
        method='GET',
        response_check=lambda response: response.status_code == 200,
        dag=dag,
    )

    func_calculate_quality_measures = SimpleHttpOperator(
        task_id='func_calculate_quality_measures',
        http_conn_id='http_default',
        endpoint='func_calculate_quality_measures',
        method='GET',
        response_check=lambda response: response.status_code == 200,
        dag=dag,
    )

    func_er_clustering = SimpleHttpOperator(
        task_id='func_er_clustering',
        http_conn_id='http_default',
        endpoint='func_er_clustering',
        method='GET',
        response_check=lambda response: response.status_code == 200,
        dag=dag,
    )

    func_file_extraction >> func_er_with_blocking >> func_er_with_brute_force >> func_calculate_quality_measures >> func_er_clustering