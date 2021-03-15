name := "hw7_spark_streaming"

version := "0.1"

scalaVersion := "2.12.4"

val sparkVersion = "3.0.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion % "provided"
)
