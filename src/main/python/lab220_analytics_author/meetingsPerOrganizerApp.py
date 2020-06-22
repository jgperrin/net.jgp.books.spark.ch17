"""
 Author/Organizer analytics.

 @author rambabu.posa
"""
from pyspark.sql import (SparkSession, functions as F)

def main(spark):
    df = spark.read.format("delta") \
        .load("/tmp/delta_grand_debat_events")

    df = df.groupBy(F.col("authorType")) \
        .count() \
        .orderBy(F.col("authorType").asc_nulls_last())

    df.show(25, 0, False)
    df.printSchema()

if __name__ == "__main__":
    # Creates a session on a local master
    # To use Databricks Delta Lake, we should add delta core packages to SparkSession
    spark = SparkSession.builder \
        .appName("JSON Lines to Dataframe") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:0.7.0") \
        .master("local[*]").getOrCreate()

    # setting log level, update this as per your requirement
    spark.sparkContext.setLogLevel("warn")

    main(spark)
    spark.stop()