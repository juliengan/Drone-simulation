name := "PeaceLand_Analysis"

version := "1.0"

scalaVersion := "2.12.10"

val kafkaVer = "2.3.0"

//enablePlugins(ScalaJSPlugin)
// This is an application with a main method
//scalaJSUseMainModuleInitializer := true

libraryDependencies ++= Seq(
  "org.scalatest" % "scalatest_2.12" % "3.1.0" % "test",
  "com.github.stevenchen3" %% "scala-faker" % "0.1.1",
  "com.typesafe.play" %% "play-json" % "2.9.2",
  "org.apache.kafka" %% "kafka" % "2.7.0",
  "org.apache.spark" %% "spark-core" % "3.1.1",
  "org.apache.spark" %% "spark-sql" % "3.1.1",
  "org.plotly-scala" %% "plotly-render" % "0.8.2",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.1.1",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.5",
  "com.google.code.gson" % "gson" % "2.7",
  "org.scalatest" %% "scalatest" % "3.0.8",
  "org.apache.kafka" % "kafka-clients" % "2.7.0",
  "org.apache.kafka" %% "kafka-streams-scala" % "2.0.1",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  //"org.scala-js" %%% "scalajs-dom" % "2.1.0",
  //"org.scala-js" %% "scalajs-env-jsdom-nodejs" % "1.0.0",
  //"com.lihaoyi" %%% "utest" % "0.7.4" % "test"
)

//testFrameworks += new TestFramework("utest.runner.Framework")

//jsEnv := new org.scalajs.jsenv.jsdomnodejs.JSDOMNodeJSEnv()
