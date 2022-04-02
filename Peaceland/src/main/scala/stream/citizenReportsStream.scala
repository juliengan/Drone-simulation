package stream

import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.codehaus.jackson.JsonParser.NumberType

import java.sql.Struct
import scala.compat.java8.FunctionConverters.enrichAsJavaFunction


object citizenReportsStream {

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
        val data = spark
          .readStream
          .format("org.apache.spark.sql.kafka010.KafkaSourceProvider")
          .option("kafka.bootstrap.servers", "localhost:9092")
          .option("subscribe", "topic-0")
          .option("startingOffsets", "earliest")
          .option("failOnDataLoss", false)
          .load()

        data.printSchema()

        // Extracts stream as a dataframe
        val dataDF = data.selectExpr("CAST(value AS STRING)")

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


        val towrite = dataDF.select(from_json($"value", schema).as("report"))
        val flat = towrite.select($"report.id", $"report.emotion", $"report.behavior", $"report.pscore", $"report.datetime", $"report.lat", $"report.lon", $"report.words".cast("string"))

        // Saves the stream as crc files we analyse
        flat
          .writeStream
          .format("kafka")
          .option("format", "append")
          .trigger(Trigger.ProcessingTime("25 seconds"))
          .option("sep", ",")
          .option("checkpointLocation", "resources/delete")
          .option("path", "resources/citizenReport/csv")
          .outputMode("append")
          .start()
          .awaitTermination();
    }


    /**
     * Serialization/Deserialization of the report
     */

    /*val props = Map("schema.registry.url" -> "http://schema-registry:8081")

    implicit def keySerde[K >: Null](implicit krf: KeyRecordFormat[K]): Serde[K] = {
        val avroKeySerde = new GenericAvroSerde
        avroKeySerde.configure(props.asJava, true)
        avroKeySerde.forCaseClass[K]
    }

    implicit def valueSerde[V >: Null](implicit vrf: ValueRecordFormat[V]): Serde[V] = {
        val avroValueSerde = new GenericAvroSerde
        avroValueSerde.configure(props.asJava, false)
        avroValueSerde.forCaseClass[V]
    }

    implicit class CaseClassSerde(inner: Serde[GenericRecord]) {
        def forCaseClass[T >: Null](implicit rf: RecordFormat[T]): Serde[T] = {
            Serdes.fromFn(
                (topic, data) => inner.serializer().serialize(topic, rf.to(data)),
                (topic, bytes) => Option(rf.from(inner.deserializer().deserialize(topic, bytes)))
            )
        }
    }*/
}
