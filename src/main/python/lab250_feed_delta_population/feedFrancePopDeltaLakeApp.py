"""
  Export data into Delta Lake

  @author rambabu.posa
"""
import os
import logging
from pyspark.sql import (SparkSession, functions as F)

def get_absolute_file_path(path, filename):
    current_dir = os.path.dirname(__file__)
    relative_path = f"{path}{filename}"
    absolute_file_path = os.path.join(current_dir, relative_path)
    return absolute_file_path

def main(spark):
    path = "../../../../data/france_population_dept/"
    filename = "population_dept.csv"
    absolute_file_path = get_absolute_file_path(path, filename)

    # Reads a CSV file, called population_dept.csv,
    # stores it in a dataframe
    df = spark.read.format("csv") \
        .option("inferSchema", True) \
        .option("header", True) \
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

    logging.warning("{} rows updated.".format(df.count()))

if __name__ == "__main__":
    # Creates a session on a local master
    # To use Databricks Delta Lake, we should add delta core packages to SparkSession
    spark = SparkSession.builder \
        .appName("Load France's population dataset and store it in Delta") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:0.7.0") \
        .master("local[*]").getOrCreate()

    # setting log level, update this as per your requirement
    spark.sparkContext.setLogLevel("warn")

    main(spark)
    spark.stop()

