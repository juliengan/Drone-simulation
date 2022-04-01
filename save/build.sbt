name := "Producer"

version := "1.0"

scalaVersion := "2.12.10"

//libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.5"
libraryDependencies += "com.google.code.gson" % "gson" % "2.7"
//libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.7.0"
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3"