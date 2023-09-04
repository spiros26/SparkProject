from pyspark.sql import SparkSession
import time

spark = SparkSession.builder.appName("rdd-query2").getOrCreate()

sc = spark.sparkContext

'''
find percentage of users that have given totally
average rating above 3 to movies
'''

t1 = time.time()

total_users = sc.textFile("hdfs://master:9000/files/ratings.csv"). \
		map(lambda x : x.split(",")[0]).distinct().count()

above3_users = sc.textFile("hdfs://master:9000/files/ratings.csv"). \
		map(lambda x : (x.split(",")[0], (float(x.split(",")[2]), 1))). \
		reduceByKey(lambda x,y : (x[0]+y[0], x[1]+y[1])). \
		map(lambda x : (x[0], x[1][0] / x[1][1])). \
		filter(lambda x : x[1] > 3).count()

res = str((above3_users / total_users) * 100)

t2 = time.time()

print("Percentage of users with average rating >3.0: " + res + '%')

print("Query2 with RDD API, runtime in seconds: %.4f"%(t2-t1))
