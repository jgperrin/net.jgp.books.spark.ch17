"""
 Dept analytics.

 @author rambabu.posa
"""
from pyspark.sql import (SparkSession, functions as F)

# Creates a session on a local master
# To use Databricks Delta Lake, we should add delta core packages to SparkSession
spark = SparkSession.builder \
    .appName("Counting the number of meetings per department") \
    .config("spark.jars.packages", "io.delta:delta-core_2.11:0.6.0") \
    .master("local[*]").getOrCreate()

df = spark.read.format("delta") \
    .load("/tmp/delta_grand_debat_events")

df = df.groupBy(F.col("authorDept")) \
    .count() \
    .orderBy(F.col("count").desc_nulls_first())

df.show(25)
df.printSchema()

spark.stop()