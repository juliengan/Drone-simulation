package producer

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord,ProducerConfig, RecordMetadata}


import java.time.Instant
import java.net.URI
import java.util.Properties


object Producer {
  // This producer sends the record to the stream eg : citizen report generated in drone object
  //testing stuff
  case class Id[Resource](value: String) extends AnyVal

  case class User(id: Id[User], updatedOn: Instant, image: URI, nickname: String, verified: Boolean, deleted: Boolean)

  case class Post(id: Id[Post], updatedOn: Instant, author: Id[User], text: String, image: URI, deleted: Boolean)

  case class Like(userId: Id[User], postId: Id[Post], updatedOn: Instant, unliked: Boolean)

  case class Comment(id: Id[Comment], postId: Id[Post], updatedOn: Instant, author: Id[User], text: String, deleted: Boolean)


  /**
   * Producer sends the record, i.e. the citizen report generated in drone.scala
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
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

    // Create a record
    val record = new ProducerRecord[String, String]("test", key, drone.jsonReport(10))
    //val record = new ProducerRecord[String, String]("test", key, "Hello Kafka")
    // send the data
    producer.send(record, (recordMetadata: RecordMetadata, exception: Exception) => {
      if (exception != null) {
        exception.printStackTrace()
      } else {
        println(s"\n\n Metadata about the sent record: $recordMetadata, ${record.value()} \n\n")
      }
    })

    // close the producer
    producer.close()
  }
}