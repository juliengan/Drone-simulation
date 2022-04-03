package stream

import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig, Topology}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._

object citizenReportsStream {

    /**
     * It streams the citizen report from drone to the analysis job
     * @param args
     */
    def main(args: Array[String]) {

        // Creates spark session using hdfs cluster
        val spark = SparkSession
          .builder()
          .appName("Peaceland")
          .master("local[*]")
          .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")

        import spark.implicits._

        // Spark reads the stream of citizenReport the producer sent
        val df = spark
          .readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", "localhost:9092")
          .option("subscribe", "__consumer_offsets")
          .option("startingOffsets", "earliest")
          .option("failOnDataLoss", false)
          .load()

        df.printSchema()

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
        // Extracts stream as a dataframe
        val report = df.selectExpr("CAST(value AS STRING)")
          .select(from_json($"value", schema).as("report"))
          .select("report.*")

        // Creates a schema for citizen information
        val subschema = new StructType()
          .add("name", StringType)
          .add("score", StringType)



        // Saves the stream as crc files we analyse
        report
          .writeStream
          .format("console")
          .outputMode("append")
          .trigger(Trigger.ProcessingTime("25 seconds"))
          .start()
          .awaitTermination();

        df.selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value")
          .writeStream
          .format("kafka")
          .outputMode("append")
          .option("kafka.bootstrap.servers", "localhost:9092")
          .option("topic", "__consumer_offsets_data")
          .start()
          .awaitTermination()
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
