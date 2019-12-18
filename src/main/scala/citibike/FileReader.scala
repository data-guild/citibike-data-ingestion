package citibike

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


object FileReader {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder()
      .appName("CitibikeHDFSReader")
      .master("local[*]")
      .getOrCreate()

    val sdf_parquet = spark.read.parquet(Constants.STATIONS_HDFS_PATH)
    sdf_parquet.show()

    val cdf_parquet = spark.read.parquet(Constants.CITY_INFO_HDFS_PATH)
    cdf_parquet.show()
  }
}
