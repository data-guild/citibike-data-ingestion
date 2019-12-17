package citibike

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, DoubleType, IntegerType, LongType, ObjectType, StringType, StructType}
import org.joda.time.DateTime


object KConsumer {
  val hdfs_master = "/Users/sgcindy.zhang/Documents/Training_Projects/hadoop-3.1.3/"
//    val hdfs_master = "hdfs://localhost:9000/"

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Citibike Data Ingestion App").master("local[*]").getOrCreate()
    val df = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test")
      .option("startingOffsets", "earliest")
      .load()

    val dfCol = df.selectExpr("CAST(value AS STRING) as raw_payload").collect()
    dfCol.map(print)

    val stationStruct = (new StructType)
      .add("id", StringType, false)
      .add("name", StringType, false)
      .add("free_bikes", IntegerType, false)
      .add("empty_slots", IntegerType, false)
      .add("latitude", DoubleType, false)
      .add("longitude", DoubleType, false)
      .add("extra", StringType, true)
      .add("timestamp", StringType, false)

    val companyStruct = (new StructType)
      .add("company", new ArrayType(StringType, false), false)

    val licenseStruct = (new StructType)
      .add("name", StringType, false)
      .add("url", StringType, false)

    val locationStruct = (new StructType)
      .add("city", StringType, false)
      .add("country", StringType, false)
      .add("latitude", DoubleType, false)
      .add("longitude", DoubleType, false)

    val netWorkStruct = (new StructType)
      .add("company", StringType, false)
      .add("href", StringType, false)
      .add("id", StringType, false)
      .add("license", licenseStruct, false)
      .add("location", locationStruct, false)
      .add("name", StringType, false)
      .add("source", StringType, false)
      .add("stations", new ArrayType(stationStruct, false), false)

    val cityStationsSchema = (new StructType)
      .add("network", netWorkStruct, false)

    df.select(col("value").cast("string")).show()

    val result = df.select(from_json(col("value").cast("string"), cityStationsSchema)
      .as("data"))
      .select("data.network.*")

    result.show()

    val path = "user/hdfs/wiki/testwiki1"

//    result.write.mode(SaveMode.Overwrite).parquet(hdfs_master + path)
    val df_parquet = spark.read.parquet(hdfs_master + path)
    df_parquet.show()

  }
}
