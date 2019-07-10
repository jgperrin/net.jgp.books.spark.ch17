package net.jgp.books.spark.ch17.lab200_feed_delta;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * JSON Lines ingestion in a dataframe.
 * 
 * For more details about the JSON Lines format, see: http://jsonlines.org/.
 * 
 * @author jgp
 */
public class FeedDeltaLakeApp {

  /**
   * main() is your entry point to the application.
   * 
   * @param args
   */
  public static void main(String[] args) {
    FeedDeltaLakeApp app =
        new FeedDeltaLakeApp();
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

    // Reads a CSV file with header, called books.csv, stores it in a dataframe
    Dataset<Row> df = spark.read().format("json")
        .load("data/france_grand_debat/20190302 EVENTS.json");

    // Shows at most 5 rows from the dataframe
    df.show(5);//, 13);
    df.printSchema();
    System.out.println(df.count());
  }
}
