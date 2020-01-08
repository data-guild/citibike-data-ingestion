package citibike

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import pureconfig.ConfigSource


object FileReader {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val appConfig = ConfigLoader.loadConfig(ConfigSource.default)

    val spark = SparkSession
      .builder()
      .appName("CitibikeHDFSReader")
      .master("local[*]")
      .getOrCreate()

    val sdf_parquet = spark
      .read
      .parquet(s"${appConfig.hdfsPath.root}/user/${appConfig.hdfsUser.user}/${appConfig.hdfsPath.city}")
    sdf_parquet.show()

    val cdf_parquet = spark
      .read
      .parquet(s"${appConfig.hdfsPath.root}/user/${appConfig.hdfsUser.user}/${appConfig.hdfsPath.station}")
    cdf_parquet.show()
  }
}
