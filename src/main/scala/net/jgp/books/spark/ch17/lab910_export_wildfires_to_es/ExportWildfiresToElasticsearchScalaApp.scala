package net.jgp.books.spark.ch17.lab910_export_wildfires_to_es

import java.io.{FileOutputStream, IOException}
import java.net.URL
import java.nio.channels.Channels

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.LoggerFactory

/**
 * Export data to Elastic Search.
 *
 * @author rambabu.posa
 *
 */
class ExportWildfiresToElasticsearchScalaApp {
  private val log = LoggerFactory.getLogger(classOf[ExportWildfiresToElasticsearchScalaApp])

  val Modis_File = "MODIS_C6_Global_24h.csv"
  val Viirs_File = "VNP14IMGTDL_NRT_Global_24h.csv"
  val Tmp_Storage = "/tmp"

  def start(): Boolean = {
    /**
     * The processing code.
     */
    if (!downloadWildfiresDatafiles) return false

    val spark = SparkSession.builder
      .appName("Wildfire data pipeline")
      .master("local[*]")
      .getOrCreate

    // Format the VIIRS dataset
    val viirsDf = spark.read
      .format("csv")
      .option("header", true)
      .option("inferSchema", true)
      .load(s"$Tmp_Storage/$Viirs_File")
      .withColumn("acq_time_min", expr("acq_time % 100"))
      .withColumn("acq_time_hr", expr("int(acq_time / 100)"))
      .withColumn("acq_time2", unix_timestamp(col("acq_date")))
      .withColumn("acq_time3", expr("acq_time2 + acq_time_min * 60 + acq_time_hr * 3600"))
      .withColumn("acq_datetime", from_unixtime(col("acq_time3")))
      .drop("acq_date", "acq_time", "acq_time_min", "acq_time_hr", "acq_time2", "acq_time3")
      .withColumnRenamed("confidence", "confidence_level")
      .withColumn("brightness", lit(null))
      .withColumn("bright_t31", lit(null))

    viirsDf.show()
    viirsDf.printSchema()

    // This piece of code shows the repartition by confidence level, so you
    // can compare when you convert the confidence as a % to a level for the
    // MODIS dataset.
    var df = viirsDf.groupBy("confidence_level").count
    var count = viirsDf.count
    df = df.withColumn("%", round(expr("100 / " + count + " * count"), 2))
    df.show()

    // Format the MODIF dataset
    val low = 40
    val high = 100

    val modisDf = spark.read
      .format("csv")
      .option("header", true)
      .option("inferSchema", true)
      .load(s"$Tmp_Storage/$Modis_File")
      .withColumn("acq_time_min", expr("acq_time % 100"))
      .withColumn("acq_time_hr", expr("int(acq_time / 100)"))
      .withColumn("acq_time2", unix_timestamp(col("acq_date")))
      .withColumn("acq_time3", expr("acq_time2 + acq_time_min * 60 + acq_time_hr * 3600"))
      .withColumn("acq_datetime", from_unixtime(col("acq_time3")))
      .drop("acq_date", "acq_time", "acq_time_min", "acq_time_hr", "acq_time2", "acq_time3")
      .withColumn("confidence_level",
        when(col("confidence").$less$eq(low), "low")
          .when(col("confidence").$greater(low).and(col("confidence").$less(high)), "nominal")
          .otherwise(lit("high")))
      .drop("confidence")
      .withColumn("bright_ti4", lit(null))
      .withColumn("bright_ti5", lit(null))

    modisDf.show()
    modisDf.printSchema()

    // This piece of code shows the repartition by confidence level, so you
    // can compare when you convert the confidence as a % to a level for the
    // MODIS dataset.
    df = modisDf.groupBy("confidence_level").count
    count = modisDf.count
    df = df.withColumn("%", round(expr("100 / " + count + " * count"), 2))
    df.show()

    val wildfireDf = viirsDf.unionByName(modisDf)
    wildfireDf.show()
    wildfireDf.printSchema()

    log.info(s"# of partitions: ${wildfireDf.rdd.getNumPartitions}")

    wildfireDf.write
      .format("org.elasticsearch.spark.sql")
      .option("es.nodes", "localhost")
      .option("es.port", "9200")
      .mode(SaveMode.Overwrite)
      .save("wildfires")

    true
  }

  /**
   * Download all data sources.
   *
   * @return
   */
  private def downloadWildfiresDatafiles: Boolean = {
    log.trace("-> downloadWildfiresDatafiles()")
    // Download the MODIS data file
    var fromFile = "https://firms.modaps.eosdis.nasa.gov/data/active_fire/c6/csv/" + Modis_File
    var toFile = s"$Tmp_Storage/$Modis_File"
    if (!download(fromFile, toFile)) return false
    // Download the VIIRS data file
    fromFile = "https://firms.modaps.eosdis.nasa.gov/data/active_fire/viirs/csv/" + Viirs_File
    toFile = s"$Tmp_Storage/$Viirs_File"
    if (!download(fromFile, toFile)) return false
    true
  }

  /**
   * Downloads data files to local temp value.
   *
   * @param fromFile
   * @param toFile
   * @return
   */
  private def download(fromFile: String, toFile: String): Boolean = {
    try {
      val website = new URL(fromFile)
      val rbc = Channels.newChannel(website.openStream)
      val fos = new FileOutputStream(toFile)
      fos.getChannel.transferFrom(rbc, 0, Long.MaxValue)
      fos.close()
      rbc.close()
    } catch {
      case e: IOException =>
        log.debug(s"Error while downloading '${fromFile}', got: ${e.getMessage}", e)
        return false
    }
    log.debug(s"${toFile} downloaded successfully.")
    true
  }

}

object ExportWildfiresToElasticsearchScalaApplication {

  /**
   * main() is your entry point to the application.
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val app = new ExportWildfiresToElasticsearchScalaApp
    app.start
  }

}