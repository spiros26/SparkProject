from pyspark.sql import SparkSession
from io import StringIO
import csv, sys, time

# Repartition join implementation

def join(x):
	# x is a list of records from both tables 1 and 2
	table1 = []
	table2 = []
	for val in x:
		if val[0] == 'table1':
			table1.append(val)
		elif val[0] == 'table2':
			table2.append(val)
		else:
			raise Exception ("Not valid record")
	res = [(y,z) for y in table1 for z in table2]
	return res

spark = SparkSession.builder.appName("repartition-join").getOrCreate()

sc = spark.sparkContext

file1 = sys.argv[1]
join_key1 = int(sys.argv[2])

file2 = sys.argv[3]
join_key2 = int(sys.argv[4])

table1 = sc.textFile(file1). \
		map(lambda x : x.split(",")). \
		map(lambda x : (x.pop(join_key1), ('table1', x)))

table2 = sc.textFile(file2). \
		map(lambda x : (x.split(","))). \
		map(lambda x : (x.pop(join_key2), ('table2', x)))

all_recs = table1.union(table2)

t1 = time.time()

join_table = all_recs. \
		groupByKey(). \
		flatMapValues(lambda x : join(x)). \
		map(lambda x : (x[0], x[1][0][1], x[1][1][1]))

'''
join_table.saveAsTextFile("hdfs://master:9000/outputs/rpjoin.csv")
'''

t2 = time.time()

for i in join_table:
	print(i)

print("Repartition join time in seconds: %.4f"%(t2-t1))
