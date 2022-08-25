from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.types import *

#Establishing a spark connection
spark=SparkSession \
	.builder \
	.appName('transformKafkaData') \
	.master('yarn') \
	.getOrCreate()

#Reading a data from json file extracted from kafka server
df=spark.read.json('/user/hadoop/clickStreamData/')

#To flatten the raw data store into respective columns in a dataframe
flatten_df=df.withColumn("value", F.split(F.regexp_replace(F.regexp_replace((F.regexp_replace("value",'\{|}',"")),'\:',','),'\"|"',"").cast("string"),','))\
.withColumn("customer_id", F.element_at("value",2))\
.withColumn("app_version", F.element_at("value",4))\
.withColumn("OS_version",F.element_at("value",6))\
.withColumn("lat",F.element_at("value",8))\
.withColumn("lon", F.element_at("value",10))\
.withColumn("page_id", F.element_at("value",12))\
.withColumn("button_id",F.element_at("value",14))\
.withColumn("is_button_click",F.element_at("value",16))\
.withColumn("is_page_view",F.element_at("value",18))\
.withColumn("is_scroll_up",F.element_at("value",20))\
.withColumn("is_scroll_down",F.element_at("value",22))\
.withColumn("date_hour",F.element_at("value",24))\
.withColumn("minutes",F.element_at("value",25))\
.withColumn("seconds",F.element_at("value",26))\
.drop("value")

#To concatenate date_hour, minutes and seconds column to make it into timestamp format
flatten_df=flatten_df.select("*",F.concat(col("date_hour"),F.lit(":"),col("minutes"),F.lit(":"),col("seconds")).alias("timestamp"))

#To remove extra characters \n from timestamp column
flatten_df = flatten_df.select("*").withColumn("timestamp",F.expr("substring(timestamp, 1, length(timestamp)-2)")).drop("date_hour").drop("minutes").drop("seconds")

#To write the flattened dataframe in csv file
flatten_df.write.option("header","true").csv('/user/hadoop/clickStream_flatten_data/')
