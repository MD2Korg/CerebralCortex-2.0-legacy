from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('PythonSQL').getOrCreate()

df = spark.read.
