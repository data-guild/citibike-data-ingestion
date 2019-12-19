package citibike

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object KConsumer {
  val topic = "citibike"

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val cityStationsSchema = getCityStationsSchema()

    val spark = SparkSession.builder().appName("Citibike Data Ingestion App").master("local[*]").getOrCreate()
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", Constants.BOOTSTRAP_SERVERS)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .load()


    val rawData = df.select(from_json(col("value").cast("string"), cityStationsSchema)
      .as("data"))
      .select("data.network.*")

    val cityInfoColumnNames = Seq("company", "href", "id", "license", "location", "name", "source")
    val cityInfo = rawData.select(cityInfoColumnNames.map(c => col(c)): _*)
    val stationsInfo = rawData.select(col("id") as "city_id",explode(col("stations")) as "station")


    val cityInfoQuery = cityInfo.writeStream
      .outputMode("append")
      .format("parquet")
      .option("path", Constants.CITY_INFO_HDFS_PATH)
      .option("checkpointLocation", Constants.CITY_CHECKPOINT_PATH)
      .start()

     val stationsQuery = stationsInfo.select("city_id", "station.*")
      .writeStream
      .outputMode("append")
      .format("parquet")
      .option("path", Constants.STATIONS_HDFS_PATH)
      .option("checkpointLocation", Constants.STATIONS_CHECKPOINT_PATH)
      .start()

    cityInfoQuery.awaitTermination()
    stationsQuery.awaitTermination()

  }

  def getCityStationsSchema(): StructType = {
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

    (new StructType)
      .add("network", netWorkStruct, false)
  }
}
