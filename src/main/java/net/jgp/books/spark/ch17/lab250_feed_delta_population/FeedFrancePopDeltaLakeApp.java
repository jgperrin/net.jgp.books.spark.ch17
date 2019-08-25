package net.jgp.books.spark.ch17.lab250_feed_delta_population;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.regexp_replace;
import static org.apache.spark.sql.functions.when;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

/**
 * 
 * @author jgp
 */
public class FeedFrancePopDeltaLakeApp {

  /**
   * main() is your entry point to the application.
   * 
   * @param args
   */
  public static void main(String[] args) {
    FeedFrancePopDeltaLakeApp app =
        new FeedFrancePopDeltaLakeApp();
    app.start();
  }

  /**
   * The processing code.
   */
  private void start() {
    // Creates a session on a local master
    SparkSession spark = SparkSession.builder()
        .appName("Load France's population dataset and store it in Delta")
        .master("local[*]")
        .getOrCreate();

    // Reads a CSV file, called population_dept.csv, stores it in a
    // dataframe
    Dataset<Row> df = spark.read().format("csv")
        .option("inferSchema", true)
        .option("header", true)
        .load("data/france_population_dept/population_dept.csv");

    df = df
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
        .drop("_c9");
    df.show(25);
    df.printSchema();

    df.write().format("delta")
        .mode("overwrite")
        .save("/tmp/delta_france_population");

    System.out.println(df.count() + " rows updated.");
  }
}
