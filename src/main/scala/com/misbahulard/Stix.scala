import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions
import com.mongodb.spark._

object Stix {

  def main(args: Array[String]): Unit = {

    // Set logger to ERROR only
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("Ids to Stix")
      .master("local[2]")
      .config("spark.mongodb.output.uri", "mongodb://10.252.108.98/stix.event")
      // .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()

    // val snortDf = spark.read.json("data/part-00000-143f4dec-d31d-4449-9510-4086bd77fa69-c000.json")
    val snortDf = spark.read.json("hdfs://10.252.108.22:9000/user/hduser/kaspa/")

    val snortDfGroup = snortDf.groupBy("src_ip", "dest_ip", "dest_port", "protocol", "alert_msg")
        .agg(functions.min("ts").alias("first_observed"),
          functions.max("ts").alias("last_observed"),
          functions.count(functions.lit(1)).alias("number_observed")).cache()

    // snortDfGroup.show()
    MongoSpark.save(snortDfGroup)

    spark.stop()

  }

}