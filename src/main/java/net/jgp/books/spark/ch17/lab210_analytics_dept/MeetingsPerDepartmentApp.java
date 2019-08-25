package net.jgp.books.spark.ch17.lab210_analytics_dept;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class MeetingsPerDepartmentApp {

  public static void main(String[] args) {
    MeetingsPerDepartmentApp app =
        new MeetingsPerDepartmentApp();
    app.start();
  }

  /**
   * The processing code.
   */
  private void start() {
    // Creates a session on a local master
    SparkSession spark = SparkSession.builder()
        .appName("Counting the number of meetings per department")
        .master("local[*]")
        .getOrCreate();

    Dataset<Row> df = spark.read().format("delta")
        .load("/tmp/delta_grand_debat_events");

    df = df.groupBy(col("authorDept")).count()
        .orderBy(col("count").desc_nulls_first());

    df.show(25);
    df.printSchema();
  }
}
