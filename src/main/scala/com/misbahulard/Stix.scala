import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions
import com.mongodb.spark._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

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

    val schema = StructType(
      Array(
        StructField("ts", StringType, nullable = true),
        StructField("company", StringType, nullable = true),
        StructField("device_id", StringType, nullable = true),
        StructField("year", IntegerType, nullable = true),
        StructField("month", IntegerType, nullable = true),
        StructField("day", IntegerType, nullable = true),
        StructField("hour", IntegerType, nullable = true),
        StructField("minute", IntegerType, nullable = true),
        StructField("second", IntegerType, nullable = true),
        StructField("protocol", StringType, nullable = true),
        StructField("ip_type", StringType, nullable = true),
        StructField("src_mac", StringType, nullable = true),
        StructField("dest_mac", StringType, nullable = true),
        StructField("src_ip", StringType, nullable = true),
        StructField("dest_ip", StringType, nullable = true),
        StructField("src_port", IntegerType, nullable = true),
        StructField("dest_port", IntegerType, nullable = true),
        StructField("alert_msg", StringType, nullable = true),
        StructField("classification", IntegerType, nullable = true),
        StructField("priority", IntegerType, nullable = true),
        StructField("sig_id", IntegerType, nullable = true),
        StructField("sig_gen", IntegerType, nullable = true),
        StructField("sig_rev", IntegerType, nullable = true),
        StructField("src_country", StringType, nullable = true),
        StructField("src_region", StringType, nullable = true),
        StructField("dest_country", StringType, nullable = true),
        StructField("dest_region", StringType, nullable = true)
      )
    )

//    val snortDf = spark.read.schema(schema).json("data/part-00000-143f4dec-d31d-4449-9510-4086bd77fa69-c000.json")
    val snortDf = spark.read.schema(schema).json("hdfs://10.252.108.22:9000/user/hduser/kaspa/")


    val snortDfGroup = snortDf.groupBy("src_ip", "dest_ip", "dest_port", "protocol", "alert_msg")
        .agg(functions.min("ts").alias("first_observed"),
          functions.max("ts").alias("last_observed"),
          functions.count(functions.lit(1)).alias("number_observed")).cache()

//    snortDfGroup.show()
    MongoSpark.save(snortDfGroup)

    spark.stop()

  }

}