package citibike

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType}


object FileReader {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder()
      .appName("CitibikeHDFSReader")
      .master("local[*]")
      .getOrCreate()

    val hdfs_master = "/Users/sgcindy.zhang/Documents/Training_Projects/hadoop-3.1.3/"
    val stationsPath = "user/hdfs/wiki/stations1"
    val cityPath = "user/hdfs/wiki/city_info"

    val sdf_parquet = spark.read.parquet(hdfs_master + stationsPath)

    sdf_parquet.show()

    val cdf_parquet = spark.read.parquet(hdfs_master + cityPath)

    cdf_parquet.show()
  }
}
