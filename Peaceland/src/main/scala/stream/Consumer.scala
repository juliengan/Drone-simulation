package stream

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer

import java.util
import java.util.{Collections, Properties}
import java.util.regex.Pattern
import scala.collection.JavaConverters._
import java.util.Properties
import scala.collection.JavaConverters.{asJavaIterableConverter, iterableAsScalaIterableConverter}
import scala.concurrent.duration.Duration

object Consumer {

  def main(args:Array[String]):Unit={
    val props: Properties = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
    props.put("auto.offset.reset", "earliest")
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "myconsumeranalytics")
    val consumer = new KafkaConsumer[String, String](props)
    val topics = "__consumer_offsets"
    try{
      consumer.subscribe(util.Arrays.asList(topics))
      consume(consumer)


    }catch {
      case e : Exception => e.printStackTrace()
    }finally{
      consumer.close()
    }

  }

  def consume(consumer: KafkaConsumer[String, String]): Unit = {
    val records: ConsumerRecords[String, String] = consumer.poll(1000)
    records.asScala.foreach { record =>
      println((s"offset =${record.offset()}, key = ${record.key()}, value =${record.value()}"))
    }
    consume(consumer)
  }

}
