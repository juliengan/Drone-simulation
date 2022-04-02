package stream

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer

import java.util.Properties

object Producer {

  /**
   * Producer sends the record, i.e. the citizen report generated in drone.scala
   * @param args
   */
  def main(args:Array[String]):Unit= {
    val props: Properties = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])

    val producer = new KafkaProducer[String, String](props)
    val record: ProducerRecord[String, String] = new ProducerRecord[String, String]("topic-0", "key", drone.jsonReport(1))
    producer.send(record, (recordMetadata: RecordMetadata, exception: Exception) => {
      if (exception != null) {
        exception.printStackTrace()
      } else {
        println(s"Metadata about the sent record: $recordMetadata, ${record.value()}" )
      }
    })
    producer.close()
  }
}

