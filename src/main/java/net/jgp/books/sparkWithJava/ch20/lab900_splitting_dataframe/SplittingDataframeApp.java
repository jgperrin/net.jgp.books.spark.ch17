package net.jgp.books.sparkWithJava.ch20.lab900_splitting_dataframe;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Splitting a dataframe to bring it back to the driver for local
 * processing.
 * 
 * @author jgp
 */
public class SplittingDataframeApp {

  /**
   * main() is your entry point to the application.
   * 
   * @param args
   */
  public static void main(String[] args) {
    SplittingDataframeApp app = new SplittingDataframeApp();
    app.start();
  }

  /**
   * The processing code.
   */
  private void start() {
    // Creates a session on a local master
    SparkSession spark = SparkSession.builder()
        .appName("Splitting a dataframe to collect it")
        .master("local")
        .getOrCreate();

    Dataset<Row> df = createRandomDataframe(spark);
    df = df.cache();
    df.show();
    
    long count = df.count();
    long inc = count / 10;
    for (long i = 0; i < count; i += inc) {
      Dataset<Row> filteredDf =
          df.where("id>=" + i + " AND id<" + (i + inc));

      List<Row> rows = filteredDf.collectAsList();
      for (Row r : rows) {
        System.out.printf("%d: %s\n", r.getAs(0), r.getString(1));
      }
    }
  }

  private static Dataset<Row> createRandomDataframe(SparkSession spark) {
    StructType schema = DataTypes.createStructType(new StructField[] {
        DataTypes.createStructField(
            "id",
            DataTypes.IntegerType,
            false),
        DataTypes.createStructField(
            "value",
            DataTypes.StringType,
            false) });

    List<Row> rows = new ArrayList<Row>();
    for (int i = 0; i < 100; i++) {
      rows.add(RowFactory.create(i, "Row #" + i));
    }
    Dataset<Row> df = spark.createDataFrame(rows, schema);
    return df;
  }
}
