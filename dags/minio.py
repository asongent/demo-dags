import sys
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.types import *

spark = SparkSession.builder.appName("minio psql s3") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio.minio.svc.cluster.local:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "rl9O0yIKbI9RUqseqCEK") \
    .config("spark.hadoop.fs.s3a.secret.key", "bd6spZUymAYOzbNRjrbmSLqZGNWAR3Um69pYAcP5") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()



def main():
    s3_location="s3a://spark/input/";
    iris = spark.read.format("csv").option("inferSchema","true").load(s3_location).toDF('SEPAL_LENGTH','SEPAL_WIDTH','PETAL_LENGTH','PETAL_WIDTH','CLASS_NAME');
    ms=iris.groupBy("CLASS_NAME").count()
    ms.coalesce(1).write.format("parquet").mode('overwrite').save("s3a://spark/output/")
    # Database connection properties
    jdbc_url = "jdbc:postgresql://postgresql.postgresql.svc.cluster.local:5432/spark_db"
    properties = {
        "user": "postgres",
        "password": "postgresPassword",
        "driver": "org.postgresql.Driver"
    }
    # Write the aggregated result to PostgreSQL
    ms.write.jdbc(url=jdbc_url, table="sample_table", mode="append", properties=properties)
main()
