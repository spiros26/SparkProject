from pyspark.sql import SparkSession
import sys, time

spark = SparkSession.builder.appName("spark-sql-query1").getOrCreate()

'''
from 2000 and afterwards, find the most profitable
movie for each year. (Not considering movies with
empty release date, 0 earnings or 0 budget.

profit = [(earnings-budget)/budget]*100
'''

arg = sys.argv[1]

t1 = time.time()

if arg == 'csv':
	movies = spark.read.format("csv").options(header = "false", delimiter = ",", inferSchema = "true").load("hdfs://master:9000/files/movies.csv")
elif arg == 'parquet':
	movies = spark.read.parquet("hdfs://master:9000/files/movies.parquet")
else:
        raise Exception ("Format not available. Use csv or parquet.")

movies.registerTempTable("movies")

sqlString = "select year(_c3) as Year, (_c1) as Movie, maxp as Profit \
		from \
		(select *, max((_c6-_c5)*100/(_c5)) \
		OVER (PARTITION BY year(_c3)) \
		AS maxp from movies) M \
		where _c6 != 0 and _c5 != 0 and (100*(_c6-_c5)/(_c5)) == maxp and year(_c3) > 1999 \
		order by year(_c3) ASC"

res = spark.sql(sqlString)

'''
res.write.csv("hdfs://master:9000/outputs/s1.csv")
'''

t2 = time.time()

res.show()

print("Query1 with SparkSQL API, runtime in seconds: %.4f"%(t2-t1))
