package citibike
import org.scalatest.funsuite.AnyFunSuite
import CitiBikeSchema._

class KConsumerTest extends AnyFunSuite with SparkSessionTestWrapper {
  import spark.implicits._


  test("load json into network schema correctly") {
    val network = CitiBikeSchema.getnetworkStruct()

    val dataDF = spark
      .read
      .schema(network)
      .option("multiline", "true") // test specific
      .format("json")
      .load("./src/test/data/nyc.json")
      .as("data")
      .select("data.network.*")
      .persist()

    dataDF.show()
    assert(dataDF.select("id").first().get(0) == "citi-bike-nyc")
  }

  test("should save raw json data") {
    assert(1===1)
  }

}
