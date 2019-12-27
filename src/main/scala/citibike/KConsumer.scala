package citibike

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import pureconfig.ConfigSource


object KConsumer {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val appConfig = ConfigLoader.loadConfig(ConfigSource.default)
    val network = CitiBikeSchema.getnetworkStruct()

    val spark = SparkSession
      .builder()
      .appName("Citibike Data Ingestion App")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val rawData = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers",
        s"${appConfig.kafka.server}:${appConfig.kafka.port}")
      .option("subscribe", appConfig.topic.name)
      .load()
      .select(from_json(col("value").cast("string"), network)
        .as("data"))
      .select($"data.network.*")

    val cityInfo = rawData
      .select($"*", $"location.*")
      .drop("stations", "location")

    val stations = rawData
      .select($"id" as ("cityID"), explode($"stations"))
      .select($"cityID", $"col.*", $"col.extra.*")
      .drop($"extra")

    val cityInfoQuery = cityInfo
      .writeStream
      .outputMode("append")
      .format("parquet")
      .option("path",
        s"${appConfig.hdfsPath.root}${appConfig.hdfsPath.city}")
      .option("checkpointLocation",
        s"${appConfig.hdfsPath.root}${appConfig.hdfsPath.cityCheckpoint}")
      .start()

    val stationsQuery = stations
      .writeStream
      .outputMode("append")
      .format("parquet")
      .option("path",
        s"${appConfig.hdfsPath.root}${appConfig.hdfsPath.station}")
      .option("checkpointLocation",
        s"${appConfig.hdfsPath.root}${appConfig.hdfsPath.stationCheckpoint}")
      .start()

    cityInfoQuery.awaitTermination()
    stationsQuery.awaitTermination()

  }
}
