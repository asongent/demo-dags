from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from pendulum import datetime

with DAG(dag_id="spark_conn", schedule=None, start_date=datetime(2023, 12, 11), catchup=False,
         render_template_as_native_obj=True):

    spark_conn = SparkSubmitOperator(
        task_id="spark_conn",
        conn_id="spark",
        application="/opt/airflow/dags/repo/dags/spark_conn.py",
        verbose=False
        )

spark_conn
