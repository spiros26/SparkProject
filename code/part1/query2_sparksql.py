from pyspark.sql import SparkSession
import sys, time

spark = SparkSession.builder.appName("spark-sql-query2").getOrCreate()

'''
find percentage of users that have given totally
average rating above 3 to movies
'''

arg = sys.argv[1]

t1 = time.time()

if arg == 'csv':
	df = spark.read.format("csv").options(header = "false", delimiter = ",", inferSchema = "true").load("hdfs://master:9000/files/ratings.csv")
elif arg == 'parquet':
	df = spark.read.parquet("hdfs://master:9000/files/ratings.parquet")
else:
        raise Exception ("Format not supported. Use csv or parquet.")

df.registerTempTable("ratings")

total_users = "select * from (select distinct _c0 from ratings)"

above3_users = "select * from (select _c0 from ratings group by _c0 having AVG(_c2) > 3)" 

nom = spark.sql(above3_users).count()

denom = spark.sql(total_users).count()

res = str(100*(nom/denom))

t2 = time.time()

print("Percentage of user with average ratings >3.0: " + res + '%')

print("Query2 with SparkSQL API, runtime in seconds: %.4f"%(t2-t1))
