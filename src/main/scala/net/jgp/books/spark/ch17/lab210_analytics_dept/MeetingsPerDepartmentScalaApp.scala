package net.jgp.books.spark.ch17.lab210_analytics_dept

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
 * Dept analytics.
 *
 * @author rambabu.posa
 */
object MeetingsPerDepartmentScalaApp {

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
      .appName("Counting the number of meetings per department")
      // To use Databricks Delta Lake, we should add delta core packages to SparkSession
      .config("spark.jars.packages", "io.delta:delta-core_2.11:0.6.0")
      .master("local[*]")
      .getOrCreate

    var df: Dataset[Row] = spark.read
      .format("delta")
      .load("/tmp/delta_grand_debat_events")

    df = df
      .groupBy(col("authorDept"))
      .count
      .orderBy(col("count").desc_nulls_first)

    df.show(25)
    df.printSchema()

    spark.stop
  }

}
