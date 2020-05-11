"""
 Author/Organizer analytics.

 @author rambabu.posa
"""
from pyspark.sql import (SparkSession, functions as F)

# Creates a session on a local master
# To use Databricks Delta Lake, we should add delta core packages to SparkSession
spark = SparkSession.builder \
    .appName("JSON Lines to Dataframe") \
    .config("spark.jars.packages", "io.delta:delta-core_2.11:0.6.0") \
    .master("local[*]").getOrCreate()

df = spark.read.format("delta") \
    .load("/tmp/delta_grand_debat_events")

df = df.groupBy(F.col("authorType")) \
    .count() \
    .orderBy(F.col("authorType").asc_nulls_last())

df.show(25, 0, False)
df.printSchema()

spark.stop()