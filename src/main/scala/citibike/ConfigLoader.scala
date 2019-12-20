package citibike

import pureconfig._
import pureconfig.generic.auto._

object ConfigLoader {

  case class MyConfig (kafka: Kafka,
                       hdfsPath: HdfsPath,
                       topic: KTopic)
  case class Kafka (server: String, port: Int)
  case class HdfsPath( root: String,
                       city: String,
                       cityCheckpoint: String,
                       station:String,
                       stationCheckpoint: String)
  case class KTopic (name: String)

  def loadConfig(conf:ConfigSource): MyConfig = {
    conf.load[MyConfig] match {
      case Left (ex) => throw new Exception (ex.toString)
      case Right (config) => return config
    }
  }

}

