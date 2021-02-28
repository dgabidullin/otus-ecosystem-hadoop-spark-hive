package kafka

import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.serialization.StringDeserializer


import java.util.{Collections, Properties}
import scala.collection.JavaConverters.{asJavaCollectionConverter, iterableAsScalaIterableConverter}
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.collection.immutable.HashMap

object BookConsumer {

  //  for (TopicPartition partition : records.partitions()) {
  //    List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
  //    for (ConsumerRecord<String, String> record : partitionRecords) {
  //      System.out.println(record.offset() + ": " + record.value());
  //    }
  //    long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
  //    consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
  //  }
  //
  val props = new Properties()
  props.put("bootstrap.servers", "192.168.99.102:29092")
  props.put("group.id", "consumer1")

  val consumer = new KafkaConsumer(props, new StringDeserializer, new StringDeserializer)

  try {

    consumer.subscribe(List("books").asJavaCollection)
    while (true) {


      val batchSize = 5
      val partitions = new HashMap[String, List[ConsumerRecord[String, String]]]()

      val records: ConsumerRecords[String, String] = consumer.poll(100)
      for (partition <- records.partitions().toIterator) {
        val partitionRecords = records.records(partition)
        partitionRecords.forEach {
          println
        }
        val lastOffset = partitionRecords.get(partitionRecords.size - 1).offset
        consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)))
      }


      //    ConsumerRecords<String, String> records = consumer.poll(100);
      //    for (ConsumerRecord<String, String> record : records) {
      //      buffer.add(record);
      //    }
      //    if (buffer.size() >= minBatchSize) {
      //      insertIntoDb(buffer);
      //      consumer.commitSync();
      //      buffer.clear();
      //    }
    }
  } finally {

    consumer.close()

  }

  //  consumer
  //    .poll(Duration.ofSeconds(1))
  //    .asScala
  //    .foreach { r => println(r.value()) }
}
