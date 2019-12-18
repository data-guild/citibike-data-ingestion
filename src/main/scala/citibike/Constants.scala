package citibike

object Constants {
  val BOOTSTRAP_SERVERS = "localhost:9092"
  val ROOT_PATH = "/Users/sgcindy.zhang/Documents/Training_Projects/hadoop-3.1.3/"

  val CITY_INFO_HDFS_PATH: String = ROOT_PATH  + "user/hdfs/wiki/city_info"
  val CITY_CHECKPOINT_PATH: String = ROOT_PATH  +"/tmp/checkpoint/city_info"
  val STATIONS_HDFS_PATH: String = ROOT_PATH  +"user/hdfs/wiki/stations1"
  val STATIONS_CHECKPOINT_PATH: String =ROOT_PATH  +"/tmp/checkpoint/stations1"
}
