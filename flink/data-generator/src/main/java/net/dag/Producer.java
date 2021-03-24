package net.dag;

import java.time.ZoneOffset;
import java.util.Properties;
import net.dag.datagen.UserStoreSupplier;
import net.dag.model.UserStoreEvent;
import net.dag.serializer.UserStoreSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Producer implements Runnable, AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(Producer.class);

  private volatile boolean isRunning;

  private final String brokers;

  private final String topic;

  public Producer(String brokers, String topic) {
    this.brokers = brokers;
    this.topic = topic;
    this.isRunning = true;
  }

  @Override
  public void run() {
    KafkaProducer<Long, UserStoreEvent> producer = new KafkaProducer<>(getProperties());

    Throttler throttler = new Throttler(100);

    UserStoreSupplier transactions = new UserStoreSupplier();

    while (isRunning) {

      UserStoreEvent userStoreEvent = transactions.get();

      long millis = userStoreEvent.eventTime.atZone(ZoneOffset.UTC).toInstant().toEpochMilli();

      ProducerRecord<Long, UserStoreEvent> record =
          new ProducerRecord<>(topic, null, millis, userStoreEvent.userId, userStoreEvent);

      LOG.info("{}", userStoreEvent);
      producer.send(record);

      try {
        throttler.throttle();
      } catch (InterruptedException e) {
        isRunning = false;
      }
    }

    producer.close();
  }

  @Override
  public void close() {
    isRunning = false;
  }

  private Properties getProperties() {
    final Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    props.put(ProducerConfig.RETRIES_CONFIG, 0);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, UserStoreSerializer.class);

    return props;
  }
}
