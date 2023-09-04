from pyspark.sql import SparkSession
from io import StringIO
import csv, time

def split_complex(x):
        return list(csv.reader(StringIO(x), delimiter=','))[0]

spark = SparkSession.builder.appName("rdd-query4").getOrCreate()

sc = spark.sparkContext

'''
find average synopsis words for drama movies, for 
every half-decade, beginning with year 2000
'''

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

t1 = time.time()

dramas = sc.textFile("hdfs://master:9000/files/movie_genres.csv"). \
		map(lambda x : (x.split(",")[0], x.split(",")[1])). \
		filter(lambda x : x[1] == 'Drama')

movies = sc.textFile("hdfs://master:9000/files/movies.csv"). \
		filter(lambda x : split_complex(x)[3][0:4] != '' and int(split_complex(x)[3][0:4]) > 1999 and int(split_complex(x)[3][0:4]) < 2020 and split_complex(x)[2] != ''). \
		map(lambda x : (split_complex(x)[0], (half_decade(int(split_complex(x)[3][0:4])), len(split_complex(x)[2].split(" ")))))

res = dramas.join(movies). \
	map(lambda x : (x[1][1][0], ((x[1][1][1], 1)))). \
	reduceByKey(lambda x, y : (x[0]+y[0], x[1]+y[1])). \
	sortByKey(). \
	map(lambda x : (x[0], x[1][0] / x[1][1])).collect()

'''
res.saveAsTextFile("hdfs://master:9000/outputs/r4.csv")
'''

t2 = time.time()

for i in res:
	print(i)

print("Query4 with RDD API, runtime in seconds: %.4f"%(t2-t1))
