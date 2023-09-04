from pyspark.sql import SparkSession
from io import StringIO
import sys, csv

'''
run script as:
    spark-submit parquet.py "hdfs://master:9000/files/<filename>.csv" "hdfs://master:9000/files/<filename>.parquet"
'''
spark = SparkSession.builder.appName("parquet").getOrCreate()

csv_path = sys.argv[1]

parquet_path = sys.argv[2]

data = spark.read.format('csv').options(header='false', inferSchema='true').load(csv_path)

data.write.parquet(parquet_path)

