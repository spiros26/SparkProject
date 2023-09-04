from pyspark.sql import SparkSession
import sys, time

spark = SparkSession.builder.appName("spark-sql-query5").getOrCreate()

'''
for each genre, find the user with the most ratings, his/her favourite
and least favourite movie. Ties are solved by choosing the most popular
among the movies.
'''

arg = sys.argv[1]

t1 = time.time()

if arg == 'csv':
	df_movies = spark.read.format("csv").options(header = "false", delimiter = ",", inferSchema = "true").load("hdfs://master:9000/files/movies.csv")
	df_genres = spark.read.format("csv").options(header = "false", delimiter = ",", inferSchema = "true").load("hdfs://master:9000/files/movie_genres.csv")
	df_ratings = spark.read.format("csv").options(header = "false", delimiter = ",", inferSchema = "true").load("hdfs://master:9000/files/ratings.csv")
elif arg == 'parquet':
	df_movies = spark.read.parquet("hdfs://master:9000/files/movies.parquet")
	df_genres = spark.read.parquet("hdfs://master:9000/files/movie_genres.parquet")
	df_ratings = spark.read.parquet("hdfs://master:9000/files/ratings.parquet")
else:
        raise Exception ("Format not supported. Use csv or parquet.")

df_movies.registerTempTable("movies")
df_genres.registerTempTable("genres")
df_ratings.registerTempTable("ratings")

sqlString = "select b.Genre, first(b.UserID) as UserID, first(b.No_ratings) as No_ratings, first(b.Name) as Favourite, first(b.Rating) as Fav_rating, first(b.Worst_Name) as Worst, first(b.Worst_rating) as Worst_rating \
		from \
		    (select a.Genre, a.UserID, a.Worst_rating, a.No_ratings, a.Name, a.Rating, a.FavRating, max(No_ratings) over (PARTITION by Genre) as MaxSum, a.FavName, a.Worst_Name \
		    from \
		        (select g._c1 as Genre, r._c0 as UserID, m._c1 as Name, r._c2 as Rating, \
		        count(r._c2) over (PARTITION BY g._c1, r._c0) as No_ratings, \
		        first(r._c2) over (PARTITION BY g._c1, r._c0 order by r._c2 DESC, m._c7 DESC) as FavRating, \
		        first(m._c1) over (PARTITION BY g._c1, r._c0 order by r._c2 DESC, m._c7 DESC) as FavName, \
		        first(r._c2) over (PARTITION BY g._c1, r._c0 order by r._c2 ASC, m._c7 DESC) as Worst_rating, \
		        first(m._c1) over (PARTITION BY g._c1, r._c0 order by r._c2 ASC, m._c7 DESC) as Worst_Name \
		        from genres as g inner join ratings as r on g._c0 == r._c1 inner join movies as m on m._c0 == r._c1) a \
		    ) b \
		where b.No_ratings == b.MaxSum and b.FavRating == b.Rating group by b.Genre order by b.Genre ASC"

res = spark.sql(sqlString)

'''
res.write.csv("hdfs://master:9000/outputs/s5.csv")
'''

t2 = time.time()

res.show()

print("Query5 with SparkSQL API, runtime in seconds: %.4f"%(t2-t1))

