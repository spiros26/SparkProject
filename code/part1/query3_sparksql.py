from pyspark.sql import SparkSession
import sys, time

spark = SparkSession.builder.appName("spark-sql-query3").getOrCreate()

'''
find average of ratings for every movie genre
and how many movies belong to this genre
'''

arg = sys.argv[1]

t1 = time.time()

if arg == 'csv':
	df_genres = spark.read.format("csv").options(header = "false", delimiter = ",", inferSchema = "true").load("hdfs://master:9000/files/movie_genres.csv")
	df_ratings = spark.read.format("csv").options(header = "false", delimiter = ",", inferSchema = "true").load("hdfs://master:9000/files/ratings.csv")
elif arg == 'parquet':
	df_genres = spark.read.parquet("hdfs://master:9000/files/movie_genres.parquet")
	df_ratings = spark.read.parquet("hdfs://master:9000/files/ratings.parquet")
else:
        raise Exception ("Format not supported. Use csv or parquet.")

df_genres.registerTempTable("genres")
df_ratings.registerTempTable("ratings")

sqlString = "select a.Genre as Movie_Genre, avg(b.Rating) as Avg_Rating, count(b.IDb) as Number_of_Movies from \
		(select distinct _c1 as Genre, _c0 as IDa from genres) a \
		inner join (select distinct _c1 as IDb, avg(_c2) as Rating from ratings where _c2 is not null group by _c1) b \
		on a.IDa == b.IDb \
		group by Genre \
		order by Genre"

res = spark.sql(sqlString)

'''
res.write.csv("hdfs://master:9000/outputs/s3.csv")
'''

t2 = time.time()

res.show()

print("Query3 with SparkSQL API, runtime in seconds: %.4f"%(t2-t1))
