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
from pyspark.sql.functions import col, month, concat, lit
from pyspark.sql.functions import from_unixtime, date_format
from pyspark.sql.types import DoubleType
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
    #print("=========== task 1-3 ==================")
    #joined_df.show(10)
    #joined_df_time_convert.show(10)

    
    #print("=========== task 1-4 ==================")
    #print("Number of rows in the dataframe = ", joined_df_time_convert.count())  
    #print("=======================================")
    #print("=======================================")

    
    ################### Task 3 - 3 ################################
    #create a new column called "Route"

    # Check for null values in 'Pickup_Borough' column
    df.filter(col("Pickup_Borough").isNull())

    # Check for null values in 'Dropoff_Borough' column
    df.filter(col("Dropoff_Borough").isNull())

    #df.select("Pickup_Borough").show(10)

    #df.select("Dropoff_Borough").show(10)

    #df = df.withColumn("Route", col("Pickup_Borough") + col("Dropoff_Borough"))
    route_df = df.withColumn("Route", concat(col("Pickup_Borough"),lit(" to "),col("Dropoff_Borough")))
    
    #df.select("Route").show(10)
    #Calculate total profit for each route
    route_df = route_df.withColumn("driver_total_pay", df["driver_total_pay"].cast(DoubleType()))
    
    route_profit_df = route_df.groupBy("Route") \
                   .agg({"driver_total_pay": "sum"}) \
                    .withColumnRenamed("sum(driver_total_pay)", "total_profit")
    
    route_profit_df = route_profit_df.select("Route", "total_profit")
    # Sort by total_profit in descending order to get top 30 routes
    top_30_routes = route_profit_df.orderBy(col("total_profit").desc()).limit(30)

    # Increase the maximum width for column truncation
    spark.conf.set("spark.sql.repl.eagerEval.maxNumRows", 1000)
    spark.conf.set("spark.sql.repl.eagerEval.truncate", 1000)
    
    #print("=========== task 2-3 ==================")
    top_30_routes.show(top_30_routes.count(),truncate = False)
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
