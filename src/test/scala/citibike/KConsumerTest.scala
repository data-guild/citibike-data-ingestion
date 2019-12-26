package citibike

import org.scalatest.funsuite.AnyFunSuite
import CitiBikeSchema._
import org.apache.spark.sql.functions._

class KConsumerTest extends AnyFunSuite with SparkSessionTestWrapper {

  import spark.implicits._

  test("flattern stations object and add cityID column") {
    val network = CitiBikeSchema.getnetworkStruct()

    val dataDF = spark
      .read
      .schema(network)
      .option("multiline", "true") // test specific
      .format("json")
      .load("./src/test/data/nyc.json")
      .select($"network.*")
      .persist()

    val stations = dataDF
      .select($"id" as ("cityID"), explode($"stations"))
      .select($"cityID", $"col.*", $"col.extra.*")
      .drop($"extra")

    assert(stations.select($"cityID").first().get(0) === "citi-bike-nyc")
    assert(stations.select($"empty_slots").first().get(0) === 30)
    assert(stations.select($"free_bikes").first().get(0) === 8)
    assert(stations.select($"id").first().get(0) === "abcdefg")
    assert(stations.select($"latitude").first().get(0) === 40.7)
    assert(stations.select($"longitude").first().get(0) === -73.9)
    assert(stations.select($"name").first().get(0) === "station name")
    assert(stations.select($"timestamp").first().get(0) === "2019-12-23T06:50:18.00Z")
    assert(stations.select($"address").first().get(0) === null)
    assert(stations.select($"last_updated").first().get(0) === 1577077407)
    assert(stations.select($"renting").first().get(0) === 1)
    assert(stations.select($"returning").first().get(0) === 1)
    assert(stations.select($"uid").first().get(0) === "3328")

  }

  test("extract cityInfo without stations") {
    val network = CitiBikeSchema.getnetworkStruct()

    val dataDF = spark
      .read
      .schema(network)
      .option("multiline", "true") // test specific
      .format("json")
      .load("./src/test/data/nyc.json")
      .select($"network.*")
      .persist()

    val city = dataDF
      .select($"*", $"location.*")
      .drop("stations", "location")

    assert(city.select($"company").first().get(0) === Array("company a", "company b"))
    assert(city.select($"gbfs_href").first().get(0) === "https://main.html")
    assert(city.select($"href").first().get(0) === "/v2/networks/citi-bike-nyc")
    assert(city.select($"id").first().get(0) === "citi-bike-nyc")
    assert(city.select($"name").first().get(0) === "Citi Bike")
    assert(city.select($"city").first().get(0) === "New York, NY")
    assert(city.select($"country").first().get(0) === "US")
    assert(city.select($"latitude").first().get(0) === 40.71)
    assert(city.select($"longitude").first().get(0) === -74.00)

  }

}
