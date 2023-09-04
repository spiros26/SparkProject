from pyspark.sql import SparkSession
from io import StringIO
import csv, time

def split_complex(x):
        return list(csv.reader(StringIO(x), delimiter=','))[0]

spark = SparkSession.builder.appName("rdd-query5").getOrCreate()

sc = spark.sparkContext

'''
for each genre, find the user with the most ratings, his/her favourite
and least favourite movie. Ties are solved by choosing the most popular
among the movies.
'''

t1 = time.time()

genres = sc.textFile("hdfs://master:9000/files/movie_genres.csv"). \
		map(lambda x : (x.split(",")[0], x.split(",")[1]))

ratings = sc.textFile("hdfs://master:9000/files/ratings.csv"). \
		map(lambda x : (x.split(",")[1], (x.split(",")[0], float(x.split(",")[2]))))

top_per_genre = genres.join(ratings). \
		map(lambda x : ((x[1][0],x[1][1][0]), 1)). \
		reduceByKey(lambda x,y : x+y). \
		map(lambda x : (x[0][0], (x[0][1], x[1]))). \
		reduceByKey(lambda x,y : x if (x[1] >= y[1]) else y). \
		map(lambda x : ((x[0], x[1][0]), x[1][1]))
		
movie_popularity = sc.textFile("hdfs://master:9000/files/movies.csv"). \
			map(lambda x : (split_complex(x)[0], (split_complex(x)[1], float(split_complex(x)[7]))))

movie_ratings = ratings.join(movie_popularity). \
		map(lambda x : (x[0], (x[1][0][0], x[1][1][0], x[1][0][1], x[1][1][1]))). \
		join(genres). \
		map(lambda x : ((x[1][1], x[1][0][0]), (x[1][0][1], x[1][0][2], x[1][0][3])))

user_fav = movie_ratings.reduceByKey(lambda x,y : x if (x[1] > y[1] or (x[1] == y[1] and x[2] > y[2])) else y)

user_least_fav = movie_ratings.reduceByKey(lambda x,y : x if (x[1] < y[1] or (x[1] == y[1] and x[2] > y[2])) else y)

res = user_fav.join(user_least_fav). \
	map(lambda x : (x[0], (x[1][0][0], x[1][0][1], x[1][1][0], x[1][1][1]))). \
	join(top_per_genre). \
	map(lambda x : (x[0][0], x[0][1], x[1][1], x[1][0][0], x[1][0][1], x[1][0][2], x[1][0][3])). \
	sortBy(lambda x : x[0], ascending=True).collect()

'''
res.saveAsTextFile("hdfs://master:9000/outputs/r5.csv")
'''

t2 = time.time()

for i in res:
	print(i)

print("Query5 with RDD API, runtime in seconds: %.4f"%(t2-t1))
