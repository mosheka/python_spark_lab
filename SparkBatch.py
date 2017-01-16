from random import randint
import time
from pyspark.sql import SQLContext

#1. Create data
sc = spark.sparkContext

a = []
for user in range(1, 1000):
	for game in range(1, 100):
		date = int(round(time.time() * 1000000))
		score = randint(-10,10)
		a.append(List(user, date, score, 'demo_game'))

#2. Use RDD
rdd = sc.parallelize(List(line))
rdd.map(lambda row: (x[0], 0 if x[2] >= 0 else 1) \ 
	.groupByKey() \
	.map(lambda x : (x[0], list(x[1]))) \  
	.map(lambda x: (x[0], sum(x[1][:-11:-1]))) \ 
	.filter(lambda x: x[1] >= 7)

#3. Use Dataframe
schema = StructType([StructField('user_id', IntegerType(), False),StructField('date', IntegerType(), False),StructField('score', IntegerType(), True),StructField('demo_game', StringType(), True)])
df = sqlContext.createDataFrame(rdd, schema)		

#df.write.orc(os.path.join(tempfile.mkdtemp(), 'data'), mode = 'append', partitionBy = 'user_id')

	