package alert

import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig, Topology}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._


object Alert {

    /**
     * It streams the citizen report from drone to the analysis job
     * @param args
     */
    def main(args: Array[String]) {

        // Creates spark session using hdfs cluster
        val spark = SparkSession
          .builder
          .appName("Peaceland")
          .master("local[*]")
          .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")

        import spark.implicits._


        // Creates a schema for citizenReport
        val schema = new StructType()
          .add("id", StringType)
          .add("name", StringType)
          .add("age", StringType)
          .add("emotion", StringType)
          .add("behavior", StringType)
          .add("pscore", IntegerType)
          .add("datetime", StringType)
          .add("lat", StringType)
          .add("lon", StringType)
          .add("words", ArrayType(StringType))

        val threshold = 5

        // citizens with too low peace score (under 5/20)
        val suspiscious = spark.readStream
          .format("org.apache.spark.sql.kafka010.KafkaSourceProvider")
          .option("kafka.bootstrap.servers", "localhost:9092")
          .option("subscribe", "test")
          .option("startingOffsets", "earliest")
          .option("failOnDataLoss", false)
          .load()
          .selectExpr("CAST(value AS STRING)")
          .select(from_json($"value", schema).as("report"))
          .select("report.*")
          .filter($"pscore" < threshold)
          .writeStream
          .format("console")
          .outputMode("append")
          .trigger(Trigger.ProcessingTime("25 seconds"))
          .start()
          .awaitTermination()

        // citizens with too low peace score (under 5/20)
        val angry = spark.readStream
          .format("org.apache.spark.sql.kafka010.KafkaSourceProvider")
          .option("kafka.bootstrap.servers", "localhost:9092")
          .option("subscribe", "test")
          .option("startingOffsets", "earliest")
          .option("failOnDataLoss", false)
          .load()
          .selectExpr("CAST(value AS STRING)")
          .select(from_json($"value", schema).as("report"))
          .select("report.*")
          .filter($"emotion" === "angry" )
          .writeStream
          .format("console")
          .outputMode("append")
          .trigger(Trigger.ProcessingTime("25 seconds"))
          .start()
          .awaitTermination()



    }
}
