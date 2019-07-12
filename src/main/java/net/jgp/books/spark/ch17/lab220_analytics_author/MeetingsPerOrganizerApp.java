package net.jgp.books.spark.ch17.lab220_analytics_author;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class MeetingsPerOrganizerApp {

  public static void main(String[] args) {
    MeetingsPerOrganizerApp app =
        new MeetingsPerOrganizerApp();
    app.start();
  }

  /**
   * The processing code.
   */
  private void start() {
    // Creates a session on a local master
    SparkSession spark = SparkSession.builder()
        .appName("JSON Lines to Dataframe")
        .master("local[*]")
        .getOrCreate();

    Dataset<Row> df = spark.read().format("delta")
        .load("/tmp/delta_grand_debat_events");

    df = df.groupBy(col("authorType")).count()
        .orderBy(col("authorType").asc_nulls_last());

    df.show(25, 0, false);
    df.printSchema();
  }
}
