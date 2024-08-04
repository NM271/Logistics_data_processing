from pyspark.sql import SparkSession
from pyspark.sql.functions import *
# import argparse

def process_data():
    # establish spark session
    spark=SparkSession.builder\
        .appName("Logistics data processing")\
        .enableHiveSupport()\
        .getOrCreate()
    
    # GCS path of the file
    file_path= f"gs://logistics_orders_bucket/input_data/orders.csv"


    # Create dataframe ligistics_df by reading data from GCS bucket
    logistics_df=spark.read.format('csv').option('header', 'true').option('inferschema', 'true').load(file_path)
    
    # filter data where statu is complete

    completed_orders_df=logistics_df.filter(col("order_status")=="Completed")

    # spark.sql("CREATE DATABASE IF NOT EXISTS HIVE_DB")

    # using default database
    query=f"use default"

    spark.sql(query)
    # create hive table name completed_orders
    spark.sql(
        """CREATE TABLE IF NOT EXISTS completed_orders
            (order_id int,
            product string,
            quantity int,
            order_status string,
            order_date date)"""
    )

    # write dataframe to hive table
    completed_orders_df.select('order_id','product','quantity','order_status','order_date')\
                        .write.mode("append").insertInto("completed_orders")
    
if __name__=="__main__":
           process_data()   