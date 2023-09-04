from pyspark.sql import SparkSession
from io import StringIO
import csv, time

def split_complex(x):
        return list(csv.reader(StringIO(x), delimiter=','))[0]

spark = SparkSession.builder.appName("rdd-query1").getOrCreate()

sc = spark.sparkContext

'''
from 2000 and afterwards, find the most profitable
movie for each year. (Not considering movies with
empty release date, 0 earnings or 0 budget.

profit = [(earnings-budget)/budget]*100
'''

t1 = time.time()

res = sc.textFile("hdfs://master:9000/files/movies.csv"). \
                map(lambda x : (split_complex(x)[3][0:4], split_complex(x)[1] ,int(split_complex(x)[5]), int(split_complex(x)[6]))). \
                filter(lambda x : x[0] != '' and int(x[0]) > 1999 and x[2] > 0 and x[3] > 0). \
		map(lambda x : (x[0], (x[1], (x[3]-x[2])*100/x[2]))). \
		reduceByKey(lambda x,y : x if (x[1] > y[1]) else y). \
		sortByKey(). \
		map(lambda x : (x[0], x[1][0], x[1][1])).collect()

'''
res.saveAsTextFile("hdfs://master:9000/outputs/r1.csv")
'''

t2 = time.time()

for i in res:
	print(i)

print("Query1 with RDD API, runtime in seconds: %.4f"%(t2-t1))
