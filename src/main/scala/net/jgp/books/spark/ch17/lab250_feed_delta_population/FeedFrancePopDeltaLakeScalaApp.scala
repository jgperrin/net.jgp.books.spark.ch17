package net.jgp.books.spark.ch17.lab250_feed_delta_population

import org.apache.spark.sql.functions.{col, regexp_replace, when}
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
 * Export data into Delta Lake
 *
 * @author rambabu.posa
 */
object FeedFrancePopDeltaLakeScalaApp {

  /**
   * main() is your entry point to the application.
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {

    /**
     * The processing code.
     */
    // Creates a session on a local master
    val spark: SparkSession = SparkSession.builder
      .appName("Load France's population dataset and store it in Delta")
      // To use Databricks Delta Lake, we should add delta core packages to SparkSession
      .config("spark.jars.packages", "io.delta:delta-core_2.12:0.7.0")
      .master("local[*]")
      .getOrCreate

    // Reads a CSV file, called population_dept.csv, stores it in a
    // dataframe
    val df: Dataset[Row] = spark.read
      .format("csv")
      .option("inferSchema", true)
      .option("header", true)
      .load("data/france_population_dept/population_dept.csv")

   val df2 = df
      .withColumn("Code département",
        when(col("Code département").$eq$eq$eq("2A"), "20")
          .otherwise(col("Code département")))
      .withColumn("Code département",
        when(col("Code département").$eq$eq$eq("2B"), "20")
          .otherwise(col("Code département")))
      .withColumn("Code département",
        col("Code département").cast(DataTypes.IntegerType))
      .withColumn("Population municipale",
        regexp_replace(col("Population municipale"), ",", ""))
      .withColumn("Population municipale",
        col("Population municipale").cast(DataTypes.IntegerType))
      .withColumn("Population totale",
        regexp_replace(col("Population totale"), ",", ""))
      .withColumn("Population totale",
        col("Population totale").cast(DataTypes.IntegerType))
      .drop("_c9")

    df2.show(25)
    df2.printSchema()

    df2.write
      .format("delta")
      .mode("overwrite")
      .save("/tmp/delta_france_population")

    println(s"${df2.count} rows updated.")

    spark.stop
  }

}
