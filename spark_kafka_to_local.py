from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Establishing Spark Session
spark = SparkSession.builder.appName("KafkaRead").getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

# Reading data from Kafka Server & Topic given
lines = spark.readStream.format("kafka") \
	.option("kafka.bootstrap.servers","18.211.252.152:9092") \
	.option("subscribe","de-capstone3") \
	.option("failOnDataLoss","false") \
	.option("startingOffsets", "earliest") \
	.load()


# Casting raw data as string and aliasing
kafkaDF = lines.selectExpr("cast(key as string)","cast(value as string)")

#Wrting kafka data into json file
output = kafkaDF \
	.writeStream \
	.outputMode("append") \
	.format("json") \
	.option("truncate", "false") \
	.option("path","/user/hadoop/clickStreamData/") \
	.option("checkpointLocation", "/user/hadoop/clickstream_checkpoint/") \
	.start()

output.awaitTermination()
