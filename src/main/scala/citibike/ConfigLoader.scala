package citibike

import pureconfig._
import pureconfig.generic.auto._

object ConfigLoader {

  case class MyConfig(kafka: Kafka)
  case class Kafka(server: String, port: Int)

  def loadConfig(conf:ConfigSource): MyConfig = {
    conf.load[MyConfig] match {
      case Left (_) => throw new Exception ("Error loading config")
      case Right (config) => return config
    }
  }

}

