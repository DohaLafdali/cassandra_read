from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.cassandra.sensors.record import CassandraRecordSensor
from airflow.providers.apache.cassandra.sensors.table import CassandraTableSensor


def handle_new_user_record():

    print("New user record added to the users table!")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 8),
    'retries': 1,
    'table':'test_dna.users',
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'cassandra_sensor_dag',
    default_args=default_args,
    description='DAG listening for insertions in Cassandra table',
    schedule_interval=timedelta(minutes=5),
)


cassandra_sensor = CassandraTableSensor(
    task_id="cassandra_sensor",
    table="users",
    cassandra_conn_id="cassandra_conn",
    dag=dag,
)

cassandra_sensor_record = CassandraRecordSensor(
    task_id="cassandra_sensor_record",
    table="users",
    cassandra_conn_id="cassandra_conn",
    keys=["user_id"],
    mode="poke",
    poke_interval=60,
    timeout=600,
    dag=dag,
)

handle_record_task = PythonOperator(
    task_id='handle_new_user_record',
    python_callable=handle_new_user_record,
    dag=dag,
)


cassandra_sensor >> cassandra_sensor_record >>handle_record_task