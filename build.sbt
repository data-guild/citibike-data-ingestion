name := "citibike-data-ingestion"

version := "1.0"

scalaVersion := "2.12.10"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.4"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.4"
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-10_2.12" % "2.4.4"
libraryDependencies += "org.apache.spark" % "spark-sql-kafka-0-10_2.12" % "2.4.4"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.12" % "2.4.4"