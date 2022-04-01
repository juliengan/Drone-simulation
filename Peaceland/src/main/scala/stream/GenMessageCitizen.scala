package stream

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.codehaus.jackson.JsonParser.NumberType

import java.sql.Struct


object GenMessageCitizen {

    def main(args: Array[String]) {

        val spark = SparkSession
          .builder
          .appName("Peaceland")
          .master("local[*]")
          .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")

        import spark.implicits._

        val data = spark
          .readStream
          .format("org.apache.spark.sql.kafka010.KafkaSourceProvider")
          .option("kafka.bootstrap.servers", "localhost:9092")
          .option("subscribe", "topic-0")
          .option("startingOffsets", "earliest")
          .option("failOnDataLoss", false)
          .load()

        data.printSchema()

        val dataDF = data.selectExpr("CAST(value AS STRING)")

        // used for alerts
        val subschema = new StructType()
          .add("name", StringType)
          .add("score", StringType)

        val schema = new StructType()
          .add("id", StringType)
          .add("emotion", StringType)
          .add("behavior", StringType)
          .add("pscore", StringType)
          .add("datetime", StringType)
          .add("lat", StringType)
          .add("lon", StringType)
          .add("words", ArrayType(StringType))

        val towrite = dataDF.select(from_json($"value", schema).as("message"))
        val flat = towrite.select($"message.id", $"message.emotion", $"message.behavior", $"message.pscore", $"message.datetime", $"message.lat", $"message.lon", $"message.words".cast("string"))

        val query = flat
          .writeStream
          .format("kafka")
          .option("format", "append")
          .trigger(Trigger.ProcessingTime("25 seconds"))
          .option("sep", ",")
          .option("checkpointLocation", "resources/delete")
          .option("path", "resources/csv")
          .outputMode("append")
          .start()
          .awaitTermination();
    }
}
