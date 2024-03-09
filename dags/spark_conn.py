from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import col, regexp_replace

spark = SparkSession.builder.appName("spark").getOrCreate()

sample_data = [{"name": "John    D.", "age": 30},
               {"name": "Alice   G.", "age": 25},
               {"name": "Bob  T.", "age": 35},
               {"name": "Eve   A.", "age": 28}]

df = spark.createDataFrame(sample_data)

df.show()
spark.stop()