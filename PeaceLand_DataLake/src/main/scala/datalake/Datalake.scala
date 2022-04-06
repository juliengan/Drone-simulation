package datalake

import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig, Topology}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{Trigger}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord,ProducerConfig}

object Datalake {
    
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

        // Spark reads the stream of citizenReport the producer sent
        val df = spark
            .readStream
            .format("org.apache.spark.sql.kafka010.KafkaSourceProvider")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "test1")
            .option("failOnDataLoss","false")
            .option("startingOffsets", "earliest")
            .load()

        df.printSchema()

        // Extracts stream as a dataframe
        val dataStringDF = df.selectExpr("CAST(value AS STRING)")

        // Creates a schema for citizenReport
        val schema = new StructType()
          .add("id", StringType)
          .add("name", StringType)
          .add("age", StringType)
          .add("emotion", StringType)
          .add("behavior", StringType)
          .add("pscore", StringType)
          .add("datetime", StringType)
          .add("lat", StringType)
          .add("lon", StringType)
          .add("words", StringType)


        val reportDF = dataStringDF.select(from_json($"value", schema).as("report"))
        .select($"report.id",$"report.name",$"report.age", $"report.emotion", $"report.behavior", $"report.pscore", $"report.datetime", $"report.lat", $"report.lon", $"report.words")

        // write stream dataframe to csv file
        println("\n\n *************************************** \n\n\t Writing to csv file \n\n ***************************************\n\n")
        reportDF
          .writeStream
          .format("csv") // supports parquet, json, orc, csv, text, avro, kafka, mysql, db2, postgresql, cassandra, redshift, dynamodb, mongodb, sql server, hbase, hive, parquet, or dataframe
          .option("path", "./data/data")
          .option("checkpointLocation", "./data/checkpoint/")
          .outputMode("append")
          .trigger(Trigger.ProcessingTime("25 seconds"))
          .start()
          .awaitTermination();
    }
}
