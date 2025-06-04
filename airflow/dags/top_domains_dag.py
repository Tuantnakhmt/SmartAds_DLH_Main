from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2024, 1, 1),
    'catchup': False
}

dag = DAG(
    'top_domains_batch_job',
    default_args=default_args,
    schedule_interval='@daily',
    description='Run top domains per category batch job',
)

run_spark_job = BashOperator(
    task_id='run_spark_top_domains',
    bash_command="""
        docker exec datalh_smartads-spark-run-9258b095f48d \
        /opt/bitnami/spark/bin/spark-submit \
        --master local[*] \
        --jars /opt/bitnami/spark/jars/mysql-connector-j-8.0.33.jar \
        /app/analytics/top_domains_by_category.py
        """,
    dag=dag,
)
# datalh_smartads-spark