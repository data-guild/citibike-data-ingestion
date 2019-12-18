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

    val hdfs_master = "/Users/sgcindy.zhang/Documents/Training_Projects/hadoop-3.1.3/"
    val path = "user/hdfs/wiki/testwiki5"

//    val df_parquet = spark
//      .read
//      .parquet("./parquet_data")

    val df_parquet = spark.read.parquet(hdfs_master + path)

    df_parquet.show()
  }
}
