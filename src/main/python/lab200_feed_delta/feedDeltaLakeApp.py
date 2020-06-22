"""
 Ingestion the 'Grand Debate' files to Delta Lake.

 @author rambabu.posa
"""
import logging
import os
from pyspark.sql import (SparkSession, functions as F)
from pyspark.sql.types import (StructType, StructField, StringType,
                               TimestampType, DoubleType, BooleanType)

# Use this if you face any 'UnicodeEncodeError' error
#import sys
#reload(sys)
#sys.setdefaultencoding('utf-8')

def get_absolute_file_path(path, filename):
    current_dir = os.path.dirname(__file__)
    relative_path = f"{path}{filename}"
    absolute_file_path = os.path.join(current_dir, relative_path)
    return absolute_file_path

def main(spark):
    path = "../../../../data/france_grand_debat/"
    filename = "20190302 EVENTS.json"
    absolute_file_path = get_absolute_file_path(path, filename)

    # Create the schema
    schema = StructType([StructField('authorId', StringType(), False),
                         StructField('authorType', StringType(), True),
                         StructField('authorZipCode', StringType(), True),
                         StructField('body', StringType(), True),
                         StructField('createdAt', TimestampType(), False),
                         StructField('enabled', BooleanType(), True),
                         StructField('endAt', TimestampType(), True),
                         StructField('fullAddress', StringType(), True),
                         StructField('id', StringType(), False),
                         StructField('lat', DoubleType(), True),
                         StructField('link', StringType(), True),
                         StructField('lng', DoubleType(), True),
                         StructField('startAt', TimestampType(), False),
                         StructField('title', StringType(), True),
                         StructField('updatedAt', TimestampType(), True),
                         StructField('url', StringType(), True)])

    # Reads a JSON file, called 20190302 EVENTS.json,
    # stores it in a dataframe
    df = spark.read.format("json") \
        .schema(schema) \
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss") \
        .load(absolute_file_path)

    df = df.withColumn("authorZipCode", F.col("authorZipCode").cast("int")) \
        .withColumn("authorZipCode", F.when(F.col("authorZipCode")< F.lit(1000), None).otherwise(F.col("authorZipCode"))) \
        .withColumn("authorZipCode", F.when(F.col("authorZipCode") >= F.lit(99999), None).otherwise(F.col("authorZipCode"))) \
        .withColumn("authorDept", F.expr("int(authorZipCode / 1000)"))

    df.show(25)
    df.printSchema()

    logging.warning(f"{df.count()} rows updated.")

    df.write.format("delta") \
        .mode("overwrite") \
        .save("/tmp/delta_grand_debat_events")

if __name__ == "__main__":
    # Creates a session on a local master
    # To use Databricks Delta Lake, we should add delta core packages to SparkSession
    spark = SparkSession.builder \
        .appName("Ingestion the 'Grand Debate' files to Delta Lake") \
        .config("spark.jars.packages", "io.delta:delta-core_2.11:0.7.0") \
        .master("local[*]").getOrCreate()

    # setting log level, update this as per your requirement
    spark.sparkContext.setLogLevel("warn")

    main(spark)
    spark.stop()




