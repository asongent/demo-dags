from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from pendulum import datetime

with DAG(dag_id="minio_conn_psql", schedule=None, start_date=datetime(2024, 3, 1), catchup=False,
         render_template_as_native_obj=True):

    spark_conn = SparkSubmitOperator(
        task_id="spark_conn",
        conn_id="spark",
        application="/opt/airflow/dags/repo/dags/minio.py",
        verbose=False,
        jars="/opt/airflow/dags/repo/dags/jars/postgresql-42.7.1.jar,/opt/airflow/dags/repo/dags/jars/aws-java-sdk-dynamodb-1.12.656.jar,/opt/airflow/dags/repo/dags/jars/aws-java-sdk-core-1.12.656.jar,/opt/airflow/dags/repo/dags/jars/aws-java-sdk-s3-1.12.587.jar,/opt/airflow/dags/repo/dags/jars/hadoop-aws-3.3.1.jar,/opt/airflow/dags/repo/dags/jars/hadoop-aws-3.3.6.jar,/opt/airflow/dags/repo/dags/jars/httpclient-4.5.14.jar,/opt/airflow/dags/repo/dags/jars/minio-8.5.7.jar"
        )

spark_conn