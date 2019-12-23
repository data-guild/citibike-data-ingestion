package citibike

import org.scalatest.funsuite.AnyFunSuite
import pureconfig._

class ConfigLoaderTest extends AnyFunSuite {
  test("Load kafka server config") {
    val myConfig = ConfigLoader.loadConfig(ConfigSource.default)
    assert(myConfig.kafka.port == 9092)
  }

  test("Load kafka server throws exception") {
    val appSource = ConfigSource.string("{ age = 33 }")
    val caught = intercept[Exception] {
      ConfigLoader.loadConfig(appSource)
    }
    assert(caught.getMessage.startsWith("Error loading config"))
  }
}
