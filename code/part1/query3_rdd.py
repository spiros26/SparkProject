from pyspark.sql import SparkSession
import time

spark = SparkSession.builder.appName("rdd-query3").getOrCreate()

sc = spark.sparkContext

'''
find average of ratings for every movie genre
and how many movies belong to this genre
'''

t1 = time.time()

genres = sc.textFile("hdfs://master:9000/files/movie_genres.csv"). \
		map(lambda x : (x.split(",")[0], x.split(",")[1]))

ratings = sc.textFile("hdfs://master:9000/files/ratings.csv"). \
                map(lambda x : (x.split(",")[1], float(x.split(",")[2])))

res = genres.join(ratings). \
	map(lambda x : ((x[1][0], x[0]), (x[1][1], 1))). \
	reduceByKey(lambda x,y : (x[0]+y[0], x[1]+y[1])). \
	map(lambda x : (x[0][0], (x[1][0] / x[1][1], 1))). \
	reduceByKey(lambda x,y : (x[0]+y[0], x[1]+y[1])). \
	sortByKey(). \
	map(lambda x : (x[0], x[1][0] / x[1][1], x[1][1])).collect()

'''
res.saveAsTextFile("hdfs://master:9000/outputs/r3.csv")
'''

t2 = time.time()

for i in res:
	print(i)

print("Query3 with RDD API, runtime in seconds: %.4f"%(t2-t1))
