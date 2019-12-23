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
      .option("multiline", "true")
      .json("./src/test/data/nyc.json")
      .as("data")
      .select("data.network.*")
      .persist()

    assert(dataDF.select("id").first().get(0) == "citi-bike-nyc")
  }

  test
}
