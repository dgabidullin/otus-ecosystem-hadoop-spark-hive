name := "hw3_kafka"

version := "0.1"

scalaVersion := "2.12.4"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % "2.6.0",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.4.0",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "org.json4s" %% "json4s-jackson" % "3.6.0",
)
