package citibike

import pureconfig._
import pureconfig.generic.auto._

object ConfigLoader {

  case class MyConfig (kafka: Kafka,
                       hdfsUser: HdfsUser,
                       hdfsPath: HdfsPath,
                       topic: KTopic)
  case class Kafka (server: String, port: Int)
  case class HdfsUser (user: String)
  case class HdfsPath( root: String,
                       city: String,
                       cityCheckpoint: String,
                       station:String,
                       stationCheckpoint: String)
  case class KTopic (name: String)

  def loadConfig(conf:ConfigSource): MyConfig = {
    conf.load[MyConfig] match {
      case Left (ex) => throw new Exception ("Error loading config" + ex.toString)
      case Right (config) => return config
    }
  }

}

