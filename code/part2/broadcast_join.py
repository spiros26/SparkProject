from pyspark.sql import SparkSession
from io import StringIO
import csv, sys, time

def join(x):
	res = []
	for v in bc_table.value:
		if v[join_key1] == x[0]:
			new = (x[0], tuple(x[1]), tuple(v[:join_key1]+v[join_key1+1:]))
			res.append(new)
	return res

spark = SparkSession.builder.appName("broadcast-join").getOrCreate()

sc = spark.sparkContext

file1 = sys.argv[1]
join_key1 = int(sys.argv[2])

file2 = sys.argv[3]
join_key2 = int(sys.argv[4])

small_table = sc.textFile(file1). \
		map(lambda x : x.split(",")).collect()

large_table = sc.textFile(file2). \
		map(lambda x : (x.split(","))). \
		map(lambda x : (x.pop(join_key2), x))

t1 = time.time()

bc_table = sc.broadcast(small_table)

join_table = large_table.flatMap(lambda x : join(x))

'''
join_table.saveAsTextFile("hdfs://master:9000/outputs/bcjoin.csv")
'''

t2 = time.time()

for i in join_table:
	print(i)

print("Broadcast join time in seconds: %.4f"%(t2-t1))
