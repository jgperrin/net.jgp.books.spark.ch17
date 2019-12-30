package net.jgp.books.spark.ch17.lab900_append_primary_key;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Appends content of a dataframe to a PostgreSQL database.
 * 
 * Check for additional information in the README.md file in the same
 * repository.
 * 
 * @author jgp
 *
 */
public class AppendDataJdbcPrimaryKeyApp {

  public static void main(String[] args) {
    AppendDataJdbcPrimaryKeyApp app = new AppendDataJdbcPrimaryKeyApp();
    app.start();
  }

  /**
   * The processing code.
   */
  private void start() {
    SparkSession spark = SparkSession.builder()
        .appName("Addition")
        .master("local[*]")
        .getOrCreate();

    Dataset<Row> df = createDataframe(spark);
    df.show(false);

    // Write in a table called ch17_lab900_pkey
    df.write()
        .mode(SaveMode.Append)
        .format("jdbc")
        .option("url", "jdbc:postgresql://localhost/spark_labs")
        .option("dbtable", "ch17_lab900_pkey")
        .option("driver", "org.postgresql.Driver")
        .option("user", "jgp")
        .option("password", "Spark<3Java")
        .save();
  }

  private static Dataset<Row> createDataframe(SparkSession spark) {
    StructType schema = DataTypes.createStructType(new StructField[] {
        DataTypes.createStructField(
            "fname",
            DataTypes.StringType,
            false),
        DataTypes.createStructField(
            "lname",
            DataTypes.StringType,
            false),
        DataTypes.createStructField(
            "id",
            DataTypes.IntegerType,
            false),
        DataTypes.createStructField(
            "score",
            DataTypes.IntegerType,
            false) });

    List<Row> rows = new ArrayList<>();
    rows.add(RowFactory.create("Matei", "Zaharia", 34, 456));
    rows.add(RowFactory.create("Jean-Georges", "Perrin", 23, 3));
    rows.add(RowFactory.create("Jacek", "Laskowski", 12, 758));
    rows.add(RowFactory.create("Holden", "Karau", 31, 369));

    return spark.createDataFrame(rows, schema);
  }
}
