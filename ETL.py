################################################  IMPORT LIBRARIES AND SET VARIABLES ################################################
from datetime import datetime
from pyspark.context import SparkContext
import pyspark.sql.functions as f

#Import glue modules
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

#Initialize contexts and session
spark_context = SparkContext.getOrCreate()
glue_context = GlueContext(spark_context)
session = glue_context.spark_session

#Parameters
glue_db = "movie-db"
glue_tbl = "read_movie_data"
s3_write_path = "s3://movies-data-bucket/write-movie-data"

################################################  EXTRACT  ################################################
#Log starting time
dt_start = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print("Start time:", dt_start)
#Read movie data to glue dynamic frame
dynamic_frame_read = glue_context.create_dynamic_frame.from_catalog(database = glue_db, table_name = glue_tbl)
#Convert dynamic frame to data frame to use standard pyspark functions
data_frame = dynamic_frame_read.toDF()

################################################  TRANSFORM (MODIFY DATA)   ################################################
#Create from a decade column  year
decade_col = f.floor(data_frame["Year"] / 10) * 10
data_frame = data_frame.withColumn("decade", decade_col)

#Group by decade: Count movies, get average rating
data_frame_aggregated = data_frame.groupby("decade").agg(
    f.count(f.col("Film")).alias('movie_count'),
    f.mean(f.col("Rotten Tomatoes %")).alias('rating mean'),
)

#Sort by the number of movies per decade
date_frame_aggregated = data_frame_aggregated.orderBy(f.desc("movie_count"))
#Print result table
data_frame_aggregated.show(10)

################################################ LOAD ################################################
#Create just 1 partition, because there is so little data
data_frame_aggregated = data_frame_aggregated.repartition(1)

#Convert back to dynamic frame
dynamic_frame_write = DynamicFrame.fromDF(data_frame_aggregated, glue_context, "dynamic_frame_write")
#Shirite data back to S3
glue_context.write_dynamic_frame.from_options(
    frame = dynamic_frame_write,
    connection_type = "s3",
    connection_options = {
        "path": s3_write_path,
    },
    format = "csv"
)

#Log end time
dt_end = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print (" Start time: ", dt_end)