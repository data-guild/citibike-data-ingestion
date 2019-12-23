package citibike

import org.apache.spark.sql.types._

object CitiBikeSchema {

  val stationStruct = (new StructType)
    .add("id", StringType, false)
    .add("name", StringType, false)
    .add("free_bikes", IntegerType, false)
    .add("empty_slots", IntegerType, false)
    .add("latitude", DoubleType, false)
    .add("longitude", DoubleType, false)
    .add("timestamp", StringType, false)

  val extraInfo = (new StructType)
    .add("address", StringType, true)
    .add("last_updated", IntegerType, false)
    .add("renting", IntegerType, false)
    .add("returning", IntegerType, false)
    .add("uid", StringType, false)

  val stationWithExtra = stationStruct
    .add("extra", extraInfo)

  val companyStruct = new ArrayType(StringType, false)

  val licenseStruct = (new StructType)
    .add("name", StringType, false)
    .add("url", StringType, false)

  val locationStruct = (new StructType)
    .add("city", StringType, false)
    .add("country", StringType, false)
    .add("latitude", DoubleType, false)
    .add("longitude", DoubleType, false)

  val networkStruct = (new StructType)
    .add("company", companyStruct, false)
    .add("href", StringType, true)
    .add("gbfs_href", StringType, true)
    .add("id", StringType, false)
    .add("license", licenseStruct, true)
    .add("location", locationStruct, false)
    .add("name", StringType, false)
    .add("source", StringType, true)
    .add("stations", new ArrayType(stationWithExtra, true), false)

  def getnetworkStruct () : StructType = {
    (new StructType).add("network", networkStruct)
  }
}
