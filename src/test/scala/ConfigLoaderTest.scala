import citibike._
import org.scalatest.funsuite.AnyFunSuite
import pureconfig._
import pureconfig.generic.auto._

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
    assert(caught.getMessage == "Error loading config")
  }
}
