import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord,ProducerConfig}

object kafkaProducerApp extends App {
    
    // configure the producer properties
    val props: Properties = new Properties()
    props.put("bootstrap.servers", "localhost:9092") // address of the broker
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer") // serializer for the key
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer") // serializer for the value
    props.put("acks", "all") // 

    // create the producer with the properties and a callback function
    val producer = new KafkaProducer[String, String](props)
    
    // Create a message
    val key: String = "akey"
    val value: String = "Hello Kafka"

    // Create a record
    val record = new ProducerRecord[String, String]("test", key, value)

    // send the data
    
    producer.send(record)
    producer.send(record)
    producer.send(record)
    producer.send(record)

    // close the producer
    producer.close()
}