package net.jgp.books.spark.ch17.lab200_feed_delta

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, when}
import org.apache.spark.sql.types.{DataTypes, StructField}

/**
 * Ingestion the 'Grand Débat' files to Delta Lake.
 *
 * @author rambabu.posa
 */
object FeedDeltaLakeScalaApp {

  /**
   * main() is your entry point to the application.
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {

    /**
     * The processing code.
     */
    // Create a session on a local master
    val spark: SparkSession = SparkSession.builder
      .appName("Ingestion the 'Grand Débat' files to Delta Lake")
      // To use Databricks Delta Lake, we should add delta core packages to SparkSession
      .config("spark.jars.packages", "io.delta:delta-core_2.12:0.7.0")
      .master("local[*]")
      .getOrCreate

    // Create the schema
    val schema = DataTypes.createStructType(Array[StructField](
      DataTypes.createStructField("authorId", DataTypes.StringType, false),
      DataTypes.createStructField("authorType", DataTypes.StringType, true),
      DataTypes.createStructField("authorZipCode", DataTypes.StringType, true),
      DataTypes.createStructField("body", DataTypes.StringType, true),
      DataTypes.createStructField("createdAt", DataTypes.TimestampType, false),
      DataTypes.createStructField("enabled", DataTypes.BooleanType, true),
      DataTypes.createStructField("endAt", DataTypes.TimestampType, true),
      DataTypes.createStructField("fullAddress", DataTypes.StringType, true),
      DataTypes.createStructField("id", DataTypes.StringType, false),
      DataTypes.createStructField("lat", DataTypes.DoubleType, true),
      DataTypes.createStructField("link", DataTypes.StringType, true),
      DataTypes.createStructField("lng", DataTypes.DoubleType, true),
      DataTypes.createStructField("startAt", DataTypes.TimestampType, false),
      DataTypes.createStructField("title", DataTypes.StringType, true),
      DataTypes.createStructField("updatedAt", DataTypes.TimestampType, true),
      DataTypes.createStructField("url", DataTypes.StringType, true)))

    // Reads a JSON file, called 20190302 EVENTS.json, stores it in a
    // dataframe
    var df = spark.read
      .format("json")
      .schema(schema)
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
      .load("data/france_grand_debat/20190302 EVENTS.json")

    df = df
      .withColumn("authorZipCode", col("authorZipCode").cast(DataTypes.IntegerType))
      .withColumn("authorZipCode",
        when(col("authorZipCode").$less(1000), null)
          .otherwise(col("authorZipCode")))
      .withColumn("authorZipCode",
        when(col("authorZipCode").$greater$eq(99999), null)
          .otherwise(col("authorZipCode")))
      .withColumn("authorDept", expr("int(authorZipCode / 1000)"))

    df.show(25)
    df.printSchema()

    df.write
      .format("delta")
      .mode("overwrite")
      .save("/tmp/delta_grand_debat_events")

    println(df.count + " rows updated.")

    spark.stop
  }

}
