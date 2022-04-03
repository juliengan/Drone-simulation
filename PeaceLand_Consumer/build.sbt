name := "PeaceLand_Consumer"

version := "1.0"

scalaVersion := "2.12.10"

libraryDependencies ++= Seq(
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.5",
  "com.google.code.gson" % "gson" % "2.7",
  "org.scalatest" %% "scalatest" % "3.0.8",
  "org.apache.kafka" % "kafka-clients" % "2.7.0",
  "ch.qos.logback" % "logback-classic" % "1.2.3"
)