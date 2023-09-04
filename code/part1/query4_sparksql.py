from pyspark.sql import SparkSession
import sys, time

spark = SparkSession.builder.appName("spark-sql-query4").getOrCreate()

def half_decade(x):
	if x >= 2000:
		if x <= 2004:
			return('2000-2004')
		elif x <= 2009:
			return('2005-2009')
		elif x <= 2014:
			return('2010-2014')
		elif x <= 2019:
			return('2015-2019')

def word_count(x):
	return len(x.split(' '))

'''
find average synopsis words for drama movies, for
every half-decade, beginning with year 2000
'''

arg = sys.argv[1]

t1 = time.time()

if arg == 'csv':
	df_movies = spark.read.format("csv").options(header = "false", delimiter = ",", inferSchema = "true").load("hdfs://master:9000/files/movies.csv")
	df_genres = spark.read.format("csv").options(header = "false", delimiter = ",", inferSchema = "true").load("hdfs://master:9000/files/movie_genres.csv")
elif arg == 'parquet':
	df_movies = spark.read.parquet("hdfs://master:9000/files/movies.parquet")
	df_genres = spark.read.parquet("hdfs://master:9000/files/movie_genres.parquet")
else:
        raise Exception ("Format not supported. Use csv or parquet.")

df_movies.registerTempTable("movies")
df_genres.registerTempTable("genres")

spark.udf.register("Half_decade", half_decade)
spark.udf.register("Word_count", word_count)

sqlString = "select a.hd as Half_decade, avg(a.words) as Avg_Summary_Length \
		from (select m._c0, m._c1, m._c2, Half_decade(year(m._c3)) as hd, Word_count(m._c2) as words, g._c0 \
			from movies as m, genres as g \
			where m._c0 == g._c0 and (g._c1) LIKE 'Drama' and year(m._c3) > 1999  and year(m._c3) < 2020 and m._c2 is not null) a \
		group by a.hd order by a.hd"

res = spark.sql(sqlString)

'''
res.write.csv("hdfs://master:9000/outputs/s4.csv")
'''

t2 = time.time()

res.show()

print("Query4 with SparkSQL API, runtime in seconds: %.4f"%(t2-t1))
