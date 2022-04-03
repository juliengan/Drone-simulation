import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig, Topology}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._

object kafkaStreamerApp {

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
          .option("subscribe", "test")
          .option("startingOffsets", "earliest")
          .load()

        df.printSchema()

        // Extracts stream as a dataframe
        val dataStringDF = df.selectExpr("CAST(value AS STRING)")

        // Creates a schema for citizen information
        val subschema = new StructType()
          .add("name", StringType)
          .add("score", StringType)

        // Creates a schema for citizenReport
        val schema = new StructType()
          .add("id", StringType)
          .add("emotion", StringType)
          .add("behavior", StringType)
          .add("pscore", StringType)
          .add("datetime", StringType)
          .add("lat", StringType)
          .add("lon", StringType)
          .add("words", ArrayType(StringType))


        val reportDF = dataStringDF.select(from_json($"value", schema).as("report"))
        .select($"report.id", $"report.emotion", $"report.behavior", $"report.pscore", $"report.datetime", $"report.lat", $"report.lon", $"report.words".cast("string"))

        // Saves the stream as crc files we analyse
        reportDF
          .writeStream
          .format("console")
          .outputMode("append")
          .trigger(Trigger.ProcessingTime("25 seconds"))
          .start()
          .awaitTermination();
    }

}