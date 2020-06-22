"""
 Export data to Elastic Search.

 @author rambabu.posa
"""
from pyspark.sql import (SparkSession, functions as F)
import logging
import requests

modis_file = "MODIS_C6_Global_24h.csv"
viirs_file = "VNP14IMGTDL_NRT_Global_24h.csv"
tmp_storage = "/tmp"

def download(fromFile, toFile):
    r = requests.get(fromFile)
    with open(toFile,'wb') as f:
        f.write(r.content)
        return True
    return False

def downloadWildfiresDatafiles():
    logging.info("-> downloadWildfiresDatafiles()")
    # Download the MODIS data file
    fromFile = "https://firms.modaps.eosdis.nasa.gov/data/active_fire/c6/csv/" + modis_file
    toFile = tmp_storage + "/" + modis_file
    if not download(fromFile, toFile): return False
    # Download the VIIRS data file
    fromFile = "https://firms.modaps.eosdis.nasa.gov/data/active_fire/viirs/csv/" + viirs_file
    toFile = tmp_storage + "/" + viirs_file
    if not download(fromFile, toFile): return False
    return True

downloadWildfiresDatafiles()

# Creates a session on a local master
spark = SparkSession.builder.appName("Wildfire data pipeline") \
    .master("local[*]").getOrCreate()

# Format the VIIRS dataset
viirsDf = spark.read \
    .format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .load("/tmp/{}".format(viirs_file))

viirsDf2 = viirsDf \
    .withColumn("acq_time_min", F.expr("acq_time % 100")) \
    .withColumn("acq_time_hr", F.expr("int(acq_time / 100)")) \
    .withColumn("acq_time2", F.unix_timestamp(F.col("acq_date"))) \
    .withColumn("acq_time3", F.expr("acq_time2 + acq_time_min * 60 + acq_time_hr * 3600")) \
    .withColumn("acq_datetime", F.from_unixtime(F.col("acq_time3"))) \
    .drop("acq_date", "acq_time", "acq_time_min", "acq_time_hr", "acq_time2", "acq_time3") \
    .withColumnRenamed("confidence", "confidence_level") \
    .withColumn("brightness", F.lit(None)) \
    .withColumn("bright_t31", F.lit(None))

viirsDf2.show()
viirsDf2.printSchema()

# This piece of code shows the repartition by confidence level, so you
# can compare when you convert the confidence as a % to a level for the
# MODIS dataset.
df = viirsDf2.groupBy("confidence_level").count()
count = viirsDf2.count()
df = df.withColumn("%", F.round(F.expr("100 / {} * count".format(count)), 2))
df.show()

# Format the MODIS dataset
low = 40
high = 100

modisDf = spark.read.format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .load("/tmp/{}".format(modis_file)) \
    .withColumn("acq_time_min", F.expr("acq_time % 100")) \
    .withColumn("acq_time_hr", F.expr("int(acq_time / 100)")) \
    .withColumn("acq_time2", F.unix_timestamp(F.col("acq_date"))) \
    .withColumn("acq_time3", F.expr("acq_time2 + acq_time_min * 60 + acq_time_hr * 3600")) \
    .withColumn("acq_datetime", F.from_unixtime(F.col("acq_time3"))) \
    .drop("acq_date", "acq_time", "acq_time_min", "acq_time_hr", "acq_time2", "acq_time3") \
    .withColumn("confidence_level", F.when(F.col("confidence") <= F.lit(low), "low")
                .when((F.col("confidence") > F.lit(low)) & (F.col("confidence") < F.lit(high)), "nominal")
                .when(F.isnull(F.col("confidence")), "high")
                .otherwise(F.col("confidence"))) \
    .drop("confidence") \
    .withColumn("bright_ti4", F.lit(None)) \
    .withColumn("bright_ti5", F.lit(None))

modisDf.show()
modisDf.printSchema()

# This piece of code shows the repartition by confidence level, so you
# can compare when you convert the confidence as a % to a level for the
# MODIS dataset.
df = modisDf.groupBy("confidence_level").count()
count = modisDf.count()
df = df.withColumn("%", F.round(F.expr("100 / {} * count".format(count)), 2))
df.show()

wildfireDf = viirsDf2.unionByName(modisDf)
wildfireDf.show()
wildfireDf.printSchema()

logging.info("# of partitions: {}".format(wildfireDf.rdd.getNumPartitions()))

wildfireDf.write.format("org.elasticsearch.spark.sql") \
    .option("es.nodes", "localhost") \
    .option("es.port", "9200") \
    .mode("overwrite") \
    .save("wildfires")

spark.stop()