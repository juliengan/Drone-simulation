package com.peaceland.producer;

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._

import java.sql.Struct


object Save {

    def main(args: Array[String]) {

    val spark = SparkSession
    .builder
    .appName("Peaceland")
    .master("local[*]")
    .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val data = spark.readStream
    .format("org.apache.spark.sql.kafka010.KafkaSourceProvider")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "topic")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", false)
    .load()


    val dataDF = data.selectExpr("CAST(value AS STRING)")
    // val dataDF = data.selectExpr("CAST(key AS STRING)", "CAST(sbtvalue AS STRING)")

    val subschema = new StructType()
    .add("name", StringType)
    .add("score", StringType)

    val schema = new StructType()
    .add("drone_id", StringType)
    .add("datetime", StringType)
    .add("lat", StringType)
    .add("lon", StringType)
    .add("citizens", ArrayType(subschema))
    .add("words", ArrayType(StringType))

    val towrite = dataDF.select(from_json($"value", schema).as("report"))
    val flat = towrite.select($"report.drone_id", $"report.datetime", $"report.lat", $"report.lon", $"report.citizens.name".cast("string"), $"report.citizens.score".cast("string"),$"report.words".cast("string"))

    val query = flat.writeStream
    .format("csv")
    .option("format", "append")
    .trigger(Trigger.ProcessingTime("2 minutes"))
    .option("sep", ";")
    .option("checkpointLocation", "resources/delete")
    .option("path", "resources/csv")
    .outputMode("append")
    .start();

    query.awaitTermination();
    }
}
