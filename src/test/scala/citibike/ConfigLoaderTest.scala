package citibike

import org.scalatest.funsuite.AnyFunSuite
import pureconfig._

class ConfigLoaderTest extends AnyFunSuite {
  test("Load kafka config") {
    val kafkaSource = ConfigSource.string(
      """
        kafka {
            server = "localhost"
            port = 9092
        }
        hdfs-user {
            user = "tester"
        }
        hdfs-path {
            root = "home/"
            city = "city-info"
            city-checkpoint = "checkpoint/city-info"
            station = "station-info"
            station-checkpoint = "checkpoint/station-info"
        }
        topic {
            name = "topic-name"
        }
      """)
    val testConfig = ConfigLoader.loadConfig(kafkaSource)

    assert(testConfig.kafka.server == "localhost")
    assert(testConfig.kafka.port == 9092)
    assert(testConfig.hdfsUser.user == "tester")
    assert(testConfig.hdfsPath.root == "home/")
    assert(testConfig.hdfsPath.city == "city-info")
    assert(testConfig.hdfsPath.cityCheckpoint == "checkpoint/city-info")
    assert(testConfig.hdfsPath.station == "station-info")
    assert(testConfig.hdfsPath.stationCheckpoint == "checkpoint/station-info")
    assert(testConfig.topic.name == "topic-name")
  }

  test("Load kafka server throws exception") {
    val appSource = ConfigSource.string("{ age = 33 }")
    val caught = intercept[Exception] {
      ConfigLoader.loadConfig(appSource)
    }
    assert(caught.getMessage.startsWith("Error loading config "))
  }
}
