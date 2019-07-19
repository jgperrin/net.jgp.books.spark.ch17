package net.jgp.books.spark.ch17.lab100_export;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExportWildfiresApp {
  private static Logger log =
      LoggerFactory.getLogger(ExportWildfiresApp.class);

  public static void main(String[] args) {
    ExportWildfiresApp app = new ExportWildfiresApp();
    app.start();
  }

  private boolean start() {
    if (!downloadWildfiresDatafiles()) {
      return false;
    }

    SparkSession spark = SparkSession.builder()
        .appName("CSV to Dataset")
        .master("local")
        .getOrCreate();
    Dataset<Row> df = spark.read().format("csv")
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
        .drop("acq_time3");
    df.show();
    df.printSchema();

    return true;
  }

  private boolean downloadWildfiresDatafiles() {
    log.trace("-> downloadWildfiresDatafiles()");
    String fromFile =
        "https://firms.modaps.eosdis.nasa.gov/data/active_fire/c6/csv/"
            + K.MODIS_FILE;
    String toFile = K.TMP_STORAGE + "/" + K.MODIS_FILE;

    if (!download(fromFile, toFile)) {
      return false;
    }

    fromFile =
        "https://firms.modaps.eosdis.nasa.gov/data/active_fire/viirs/csv/"
            + K.VIIRS_FILE;
    toFile = K.TMP_STORAGE + "/" + K.VIIRS_FILE;
    if (!download(fromFile, toFile)) {
      return false;
    }

    return true;
  }

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
    } finally {

    }

    log.debug("{} downloaded successfully.", toFile);
    return true;
  }

}
