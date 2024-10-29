"""
Refernce: Lab 4. Joining Datasets using NASDAQ data

Aim: Apply the 'join' function based on fields pickup_location and dropoff_location of rideshare_data table and the LocationID field of taxi_zone_lookup table, and rename those columns as (Pickup_Borough, Pickup_Zone, Pickup_service_zone) ,( Dropoff_Borough, Dropoff_Zone, Dropoff_service_zone). The join needs to be done in two steps. once using pickup_location and then output result is joined using dropoff_location. you will have a new dataframe (as shown below) with six new columns added to the original dataset.
"""
import sys, string
import os
import socket
import time
import operator
import boto3
import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, month, avg
from pyspark.sql.functions import from_unixtime, date_format

import pandas as pd

if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("CW_task1")\
        .getOrCreate()

    # shared read-only object bucket containing datasets
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']

    s3_endpoint_url = os.environ['S3_ENDPOINT_URL']+':'+os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")


    rideshare_data_df = spark.read.option("header", True).csv("s3a://" + "data-repository-bkt/ECS765/rideshare_2023/rideshare_data.csv")
    taxi_zone_lookup_df = spark.read.option("header", True).csv("s3a://" + "data-repository-bkt/ECS765/rideshare_2023/taxi_zone_lookup.csv")
    ## task 1-1 ##
    # Show the schema of the DataFrame
    #print("==========================")
    #print("==========================")
    #print("rideshare_data_df:")
    #rideshare_data_df.printSchema()
    #print("==========================")
    #print("==========================")
    #print("taxi_zone_lookup_df:")
    #taxi_zone_lookup_df.printSchema()
    #print("==========================")
    #print("==========================")


    ## task1-2 ##
    #Step 1: Join using pickup_location
    joined_pickup_df = rideshare_data_df.join(taxi_zone_lookup_df,rideshare_data_df["pickup_location"] == taxi_zone_lookup_df["LocationID"],"left")

    # change the column name and remove the column of LocationID 
    joined_pickup_df = joined_pickup_df.withColumnRenamed("Borough", "Pickup_Borough")
    joined_pickup_df = joined_pickup_df.withColumnRenamed("Zone", "Pickup_Zone")
    joined_pickup_df = joined_pickup_df.withColumnRenamed("service_zone", "Pickup_service_zone")
    joined_pickup_df = joined_pickup_df.drop("LocationID")

    #print("==========================")
    #print("==========================")
    #print("joined_pickup_df:")
    #joined_pickup_df.printSchema()
    #print("==========================")
    #print("==========================")
    

    # Step 2: Join using dropoff_location
    joined_df = joined_pickup_df.join(taxi_zone_lookup_df, joined_pickup_df["dropoff_location"] == taxi_zone_lookup_df["LocationID"], "left")
    
    # change the column name and remove the column of LocationID 
    joined_df = joined_df.withColumnRenamed("Borough", "Dropoff_Borough")
    joined_df = joined_df.withColumnRenamed("Zone", "Dropoff_Zone")
    joined_df = joined_df.withColumnRenamed("service_zone", "Dropoff_service_zone")
    joined_df = joined_df.drop("LocationID")


    #print("joined_df:")
    #joined_df.printSchema()
    print("==========================")
    
    
    ## task 1-3 ##
    
    #joined_df.select(from_unixtime(unix_timestamp("date"), "yyyy-MM-dd")).show(10)

    # Convert UNIX timestamp to timestamp type
    joined_df_time_convert = joined_df.withColumn("timestamp_col", from_unixtime("date"))

    # Format the timestamp to "yyyy-MM-dd" format
    joined_df_time_convert = joined_df_time_convert.withColumn("date", date_format("timestamp_col", "yyyy-MM-dd"))

    # Drop the original timestamp column if needed
    df = joined_df_time_convert.drop("timestamp_col")
   

    ###############################################################
    ################### Task 4 - 1 ################################

    avg_pay_by_time_of_day = df.groupBy("time_of_day") \
                    .agg(avg(col("driver_total_pay")).alias("avg_driver_total_pay")) 
    #                .orderBy("avg_driver_total_pay", ascending=False)

    # Show the result
    #avg_pay_by_time_of_day.show()

    ################### Task 4 - 2 ################################

    avg_trip_length_by_time_of_day = df.groupBy("time_of_day") \
                    .agg(avg(col("trip_length")).alias("avg_trip_length")) 
    #                .orderBy("avg_trip_length", ascending=False)

    # Show the result
    print("=========== task 4-2 : average_trip_length by time_of_day ===================")
    #avg_trip_length_by_time_of_day.show()

    ################### Task 4 - 3 ################################

    #Step 1: Join using time_of_day
    #new_df = avg_pay_by_time_of_day.join(avg_trip_length_by_time_of_day,avg_pay_by_time_of_day["time_of_day"] == avg_trip_length_by_time_of_day["time_of_day"],"left")

    new_df = avg_pay_by_time_of_day.join(avg_trip_length_by_time_of_day, on="time_of_day",how="inner")

    #new_df = new_df.drop("time_of_day")
    print("=========== task 4-3 : join ===================")
    new_df.show()
    
    #Step 2: Calculate avg_earning_per_mile
    new_df = new_df.withColumn("avg_earning_per_mile", col('avg_driver_total_pay')/col('avg_trip_length'))
    
    #print("=========== task 4-3 : join + new_col ===================")
    #new_df.show()
    #Step 3: Show the selected columns
    new_df = new_df.select("time_of_day", "avg_earning_per_mile").orderBy("avg_earning_per_mile", ascending=False)
    
    #print("=========== task 4-3 : average earning per mile ===================")
    new_df.show()
    ###############################################
    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")
    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)


    # Specify the output path where you want to save the CSV file
    output_path = "s3a://" + s3_bucket + "/cw_task1_" + date_time 


    # Write the DataFrame to a CSV file
    #joined_df_time_convert.write.csv(output_path, mode="overwrite")

    spark.stop()
