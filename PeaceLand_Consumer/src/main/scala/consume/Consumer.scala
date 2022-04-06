package consume

import org.apache.kafka.clients.consumer.{KafkaConsumer, ConsumerConfig,ConsumerRecords}

import java.util
import java.util.{Collections, Properties}
import java.util.regex.Pattern
import scala.collection.JavaConverters._
import java.util.Properties
import scala.collection.JavaConverters.{asJavaIterableConverter, iterableAsScalaIterableConverter}
import scala.concurrent.duration.Duration

object Consumer {
    /**
     * Consumer reads the record, i.e. the citizen report generated in drone.scala
     * @param args
     */
    def main(args:Array[String]):Unit= {
        // configure the consumer properties
        val props: Properties = new Properties()
        props.put("bootstrap.servers", "localhost:9092") // broker list
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer") // key deserializer
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer") // value deserializer
        props.put("group.id", "__consumer_offsets") // consumer group
        props.put("enable.auto.commit", "true") // commit offsets automatically
        props.put("auto.commit.interval.ms", "1000") // commit every second
        props.put("session.timeout.ms", "30000") // timeout to close consumer

        // create the consumer with the properties and a callback function
        val consumer = new KafkaConsumer[String, String](props)

        // topic name
        val topics = "test1"

        // subscribe to the topic
        consumer.subscribe(util.Arrays.asList(topics))

        // consume the messages
        while (true) {
            val records: ConsumerRecords[String, String] = consumer.poll(100)
            records.forEach(
                record => println(s"offset =${record.offset()}, key = ${record.key()}, value =${record.value()}")
            )
        }
    }
}
