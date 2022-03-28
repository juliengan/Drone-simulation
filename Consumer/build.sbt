name := "SparkProject"

version := "1.0"

scalaVersion := "2.12.10"

//libraryDependencies += "org.apache.spark" %% "spark-core" % "3.1.2"
//libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.2"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.5"
libraryDependencies += "com.google.code.gson" % "gson" % "2.7"
//libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8"
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3"
//libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.7.0"

libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.7.0"