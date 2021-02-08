package kafka

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.json4s.jackson.JsonMethods.{compact, render}
import org.json4s.JsonDSL._
import service.Book

import java.util.Properties

object BookProducer {
  val props = new Properties()
  props.put("bootstrap.servers", "localhost:29092")

  val producer = new KafkaProducer(props, new StringSerializer, new StringSerializer)

  def sendMessages(topic: String, books: Seq[Book]): Unit = {
    try {
      books.foreach {
        book =>
          val json = ("name" -> book.name) ~
            ("author" -> book.author) ~
            ("userRating" -> book.userRating) ~
            ("reviews" -> book.reviews) ~
            ("price" -> book.price) ~
            ("year" -> book.year) ~
            ("genre" -> book.genre)

          val message = compact(render(json))

          producer.send(new ProducerRecord(topic, message, message))
      }
    } finally {
      producer.close()
    }
  }
}
