package kafka

import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer

import java.time.Duration
import java.util
import java.util.Properties
import scala.collection.JavaConverters.asJavaCollectionConverter
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.collection.mutable

object BookConsumer {

  val props = new Properties()
  props.put("bootstrap.servers", "localhost:29092")
  props.put("group.id", "consumer1")

  val consumer = new KafkaConsumer(props, new StringDeserializer, new StringDeserializer)

  def printLastRecords(topic: String, recordsCountToPrint: Int): Unit = {
    try {
      val bufferPartitionRecordsToPrint =
        new mutable.HashMap[String, List[ConsumerRecord[String, String]]].withDefaultValue(List[ConsumerRecord[String, String]]())

      subscribeTo(topic)
      val bookRecords = consumer.poll(Duration.ofSeconds(3))
      val partitionsWithMaxOffsets = initPartitionsMaxOffsets()

      for (partition <- bookRecords.partitions()) {
        bookRecords.records(partition).forEach(rec => {
          val key = s"${rec.topic()}_${rec.partition()}"
          if (rec.offset() > (partitionsWithMaxOffsets.get(key) - recordsCountToPrint)) {
            bufferPartitionRecordsToPrint(key) ::= rec
            bufferPartitionRecordsToPrint.put(key, bufferPartitionRecordsToPrint(key))
          }
        })
      }
      bufferPartitionRecordsToPrint
        .keys
        .foreach(
          partition => bufferPartitionRecordsToPrint(partition)
            .foreach(println)
        )
    } finally {
      consumer.close()
    }
  }

  def initPartitionsMaxOffsets(): util.HashMap[String, Long] = {
    val partitionMaxOffsets = new util.HashMap[String, Long]()
    for (partition <- consumer.assignment()) {
      val key = s"${partition.topic()}_${partition.partition()}"
      val lastPartitionOffset = consumer.position(partition) - 1
      println(s"partition=${partition.partition()}, lastOffset=$lastPartitionOffset")
      partitionMaxOffsets.put(key, lastPartitionOffset)
    }
    partitionMaxOffsets
  }

  def subscribeTo(topic: String): Unit = {
    consumer.subscribe(List(topic).asJavaCollection)
    val isAssigned = false
    while (!isAssigned) {
      if (!consumer.assignment().isEmpty) {
        return
      }
      consumer.poll(Duration.ofMillis(0))
    }
  }
}
