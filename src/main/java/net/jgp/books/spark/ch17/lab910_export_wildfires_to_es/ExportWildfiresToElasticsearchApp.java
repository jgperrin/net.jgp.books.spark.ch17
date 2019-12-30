package net.jgp.books.spark.ch17.lab910_export_wildfires_to_es;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.from_unixtime;
import static org.apache.spark.sql.functions.isnull;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.round;
import static org.apache.spark.sql.functions.unix_timestamp;
import static org.apache.spark.sql.functions.when;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExportWildfiresToElasticsearchApp {
  private static Logger log =
      LoggerFactory.getLogger(ExportWildfiresToElasticsearchApp.class);

  public static void main(String[] args) {
    ExportWildfiresToElasticsearchApp app =
        new ExportWildfiresToElasticsearchApp();
    app.start();
  }

  private boolean start() {
    if (!downloadWildfiresDatafiles()) {
      return false;
    }

    SparkSession spark = SparkSession.builder()
        .appName("Wildfire data pipeline")
        .master("local[*]")
        .getOrCreate();

    // Format the VIIRS dataset
    Dataset<Row> viirsDf = spark.read().format("csv")
        .option("header", true)
        .option("inferSchema", true)
        .load(K.TMP_STORAGE + "/" + K.VIIRS_FILE)
        .withColumn("acq_time_min", expr("acq_time % 100"))
        .withColumn("acq_time_hr", expr("int(acq_time / 100)"))
        .withColumn("acq_time2", unix_timestamp(col("acq_date")))
        .withColumn(
            "acq_time3",
            expr("acq_time2 + acq_time_min * 60 + acq_time_hr * 3600"))
        .withColumn("acq_datetime", from_unixtime(col("acq_time3")))
        .drop("acq_date")
        .drop("acq_time")
        .drop("acq_time_min")
        .drop("acq_time_hr")
        .drop("acq_time2")
        .drop("acq_time3")
        .withColumnRenamed("confidence", "confidence_level")
        .withColumn("brightness", lit(null))
        .withColumn("bright_t31", lit(null));
    viirsDf.show();
    viirsDf.printSchema();

    // This piece of code shows the repartition by confidence level, so you
    // can compare when you convert the confidence as a % to a level for the
    // MODIS dataset.
    Dataset<Row> df = viirsDf.groupBy("confidence_level").count();
    long count = viirsDf.count();
    df = df.withColumn("%", round(expr("100 / " + count + " * count"), 2));
    df.show();

    // Format the MODIF dataset
    int low = 40;
    int high = 100;
    Dataset<Row> modisDf = spark.read().format("csv")
        .option("header", true)
        .option("inferSchema", true)
        .load(K.TMP_STORAGE + "/" + K.MODIS_FILE)
        .withColumn("acq_time_min", expr("acq_time % 100"))
        .withColumn("acq_time_hr", expr("int(acq_time / 100)"))
        .withColumn("acq_time2", unix_timestamp(col("acq_date")))
        .withColumn(
            "acq_time3",
            expr("acq_time2 + acq_time_min * 60 + acq_time_hr * 3600"))
        .withColumn("acq_datetime", from_unixtime(col("acq_time3")))
        .drop("acq_date")
        .drop("acq_time")
        .drop("acq_time_min")
        .drop("acq_time_hr")
        .drop("acq_time2")
        .drop("acq_time3")
        .withColumn(
            "confidence_level",
            when(col("confidence").$less$eq(low), "low"))
        .withColumn(
            "confidence_level",
            when(
                col("confidence").$greater(low)
                    .and(col("confidence").$less(high)),
                "nominal")
                    .otherwise(col("confidence_level")))
        .withColumn(
            "confidence_level",
            when(isnull(col("confidence_level")), "high")
                .otherwise(col("confidence_level")))
        .drop("confidence")
        .withColumn("bright_ti4", lit(null))
        .withColumn("bright_ti5", lit(null));
    modisDf.show();
    modisDf.printSchema();

    // This piece of code shows the repartition by confidence level, so you
    // can compare when you convert the confidence as a % to a level for the
    // MODIS dataset.
    df = modisDf.groupBy("confidence_level").count();
    count = modisDf.count();
    df = df.withColumn("%", round(expr("100 / " + count + " * count"), 2));
    df.show();

    Dataset<Row> wildfireDf = viirsDf.unionByName(modisDf);
    wildfireDf.show();
    wildfireDf.printSchema();

    log.info("# of partitions: {}", wildfireDf.rdd().getNumPartitions());

    wildfireDf
        .write()
        .format("org.elasticsearch.spark.sql")
        .option("es.nodes", "localhost")
        .option("es.port", "9200")
        .mode(SaveMode.Overwrite)
        .save("wildfires");

    return true;
  }

  /**
   * Download all data sources.
   * 
   * @return
   */
  private boolean downloadWildfiresDatafiles() {
    log.trace("-> downloadWildfiresDatafiles()");
    // Download the MODIS data file
    String fromFile =
        "https://firms.modaps.eosdis.nasa.gov/data/active_fire/c6/csv/"
            + K.MODIS_FILE;
    String toFile = K.TMP_STORAGE + "/" + K.MODIS_FILE;

    if (!download(fromFile, toFile)) {
      return false;
    }

    // Download the VIIRS data file
    fromFile =
        "https://firms.modaps.eosdis.nasa.gov/data/active_fire/viirs/csv/"
            + K.VIIRS_FILE;
    toFile = K.TMP_STORAGE + "/" + K.VIIRS_FILE;
    if (!download(fromFile, toFile)) {
      return false;
    }

    return true;
  }

  /**
   * Downloads data files to local temp value.
   * 
   * @param fromFile
   * @param toFile
   * @return
   */
  private boolean download(String fromFile, String toFile) {
    try {
      URL website = new URL(fromFile);
      ReadableByteChannel rbc = Channels.newChannel(website.openStream());
      FileOutputStream fos = new FileOutputStream(toFile);
      fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
      fos.close();
      rbc.close();
    } catch (IOException e) {
      log.debug("Error while downloading '{}', got: {}", fromFile,
          e.getMessage(), e);
      return false;
    }

    log.debug("{} downloaded successfully.", toFile);
    return true;
  }
}
