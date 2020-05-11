"""
  Export data into Delta Lake

  @author rambabu.posa
"""
from pyspark.sql import (SparkSession, functions as F)
import os

current_dir = os.path.dirname(__file__)
relative_path = "../../../../data/france_population_dept/population_dept.csv"
absolute_file_path = os.path.join(current_dir, relative_path)

# Creates a session on a local master
# To use Databricks Delta Lake, we should add delta core packages to SparkSession
spark = SparkSession.builder \
    .appName("Load France's population dataset and store it in Delta") \
    .config("spark.jars.packages", "io.delta:delta-core_2.11:0.6.0") \
    .master("local[*]").getOrCreate()

# Reads a CSV file, called population_dept.csv,
# stores it in a dataframe
df = spark.read.format("csv") \
    .option("inferSchema", True) \
    .option("header", True) \
    .option("encoding", "utf-8") \
    .load(absolute_file_path)

df = df.withColumn("Code département",
          F.when(F.col("Code département") == F.lit("2A"), "20")
          .otherwise(F.col("Code département"))) \
    .withColumn("Code département",
          F.when(F.col("Code département") == F.lit("2B"), "20")
           .otherwise(F.col("Code département"))) \
    .withColumn("Code département",
          F.col("Code département").cast("int")) \
    .withColumn("Population municipale",
          F.regexp_replace(F.col("Population municipale"), ",", "")) \
    .withColumn("Population municipale",
          F.col("Population municipale").cast("int")) \
    .withColumn("Population totale",
          F.regexp_replace(F.col("Population totale"), ",", "")) \
    .withColumn("Population totale",
          F.col("Population totale").cast("int")) \
    .drop("_c9")

df.show(25)
df.printSchema()

df.write.format("delta") \
    .mode("overwrite") \
    .option("encoding", "utf-8") \
    .save("/tmp/delta_france_population")

print("{} rows updated.".format(df.count()))

spark.stop()

