# Data Capture And Analysis Of Cab Rides

### Architecture:

![image](https://user-images.githubusercontent.com/60545540/186569020-39f6741a-daf2-480c-bc55-a1cc33b427b7.png)

### Approach:
1. First, captured the clickstream data from Kafka and loaded into HDFS using a stream processing framework. 
2. Next, ingested the booking data from AWS RDS to Hadoop. 
3. These are then loaded into Hive tables to make them queryable. 
4. In some cases, prepared aggregates, data is read from HDFS, processed by a framework such as Spark and written back to HDFS to create a Hive table for aggregated data to make it more queryable. 
5. These Hive tables served as the final consumption tables for stakeholders to query and derive meaningful insights from the data.


