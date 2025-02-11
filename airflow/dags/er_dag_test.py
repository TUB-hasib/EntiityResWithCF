from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.dates import days_ago

from airflow.models import Connection
from airflow.settings import Session



# Function to add HTTP connections dynamically
def add_connection(conn_id, host, port):
    session = Session()
    existing_conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()
    
    if not existing_conn:
        new_conn = Connection(
            conn_id=conn_id,
            conn_type="http",
            host=host,
            port=port
        )
        session.add(new_conn)
        session.commit()
        print(f"Added connection: {conn_id}")
    else:
        print(f"Connection {conn_id} already exists")


# Add connections for local functions
add_connection("local_func_calculate_quality_measures", "http://127.0.0.1", 8084)
# add_connection("local_func_er_clustering", "http://127.0.0.1", 8085)  
  

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

with DAG(
    'a_er_dag_test',
    default_args=default_args,
    description='test dag for entity resolution',
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['entity resolution'],
) as dag:

    # func_file_extraction = SimpleHttpOperator(
    #     task_id='func_file_extraction',
    #     http_conn_id='http_default',
    #     endpoint='func_file_extraction',
    #     method='GET',
    #     response_check=lambda response: response.status_code == 200,
    #     dag=dag,
    # )

    # func_er_with_blocking = SimpleHttpOperator(
    #     task_id='func_er_with_blocking',
    #     http_conn_id='http_default',
    #     endpoint='func_er_with_blocking',
    #     method='GET',
    #     response_check=lambda response: response.status_code == 200,
    #     dag=dag,
    # )

    # func_er_with_brute_force = SimpleHttpOperator(
    #     task_id='func_er_with_brute_force',
    #     http_conn_id='http_default',
    #     endpoint='func_er_with_brute_force',
    #     method='GET',
    #     response_check=lambda response: response.status_code == 200,
    #     dag=dag,
    # )

    func_calculate_quality_measures = SimpleHttpOperator(
        task_id='func_calculate_quality_measures',
        http_conn_id='http_default',
        endpoint='local_func_calculate_quality_measures',
        method='GET',
        response_check=lambda response: response.status_code == 200,
        dag=dag,
    )



    # func_er_clustering = SimpleHttpOperator(
    #     task_id='func_er_clustering',
    #     http_conn_id='http_default',
    #     endpoint='local_func_er_clustering',
    #     method='GET',
    #     response_check=lambda response: response.status_code == 200,
    #     dag=dag,
    # )

    func_calculate_quality_measures