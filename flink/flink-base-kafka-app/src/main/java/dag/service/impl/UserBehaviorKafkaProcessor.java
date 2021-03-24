package dag.service.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import dag.service.model.UserStoreEvent;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.sequencefile.SequenceFileWriterFactory;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Objects;
import java.util.Properties;

public class UserBehaviorKafkaProcessor extends KafkaProcessor<UserStoreEvent> {

    private static final ZoneId PARTITIONS_TIMEZONE =
            ZoneId.of("Europe/Moscow");

    private static final ObjectMapper OBJECT_MAPPER = KafkaSiteObjectMapperFactory.createObjectMapper()
            .registerModule(new KafkaSiteJacksonModule());

    private final Configuration configuration;

    public UserBehaviorKafkaProcessor() {
        configuration = new Configuration();
    }

    public static void main(String[] args) throws Exception {
        UserBehaviorKafkaProcessor processor = new UserBehaviorKafkaProcessor();
        processor.process(args);
    }

    @Override
    protected FlinkKafkaConsumer<UserStoreEvent> createKafkaConsumer(String topicName, Properties kafkaProperties) {
        FlinkKafkaConsumer<UserStoreEvent> kafkaConsumer = new FlinkKafkaConsumer<>(
                "user_behavior",
                new CustomKafkaDeserializationSchema(), kafkaProperties);
        kafkaConsumer.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserStoreEvent>(){
            @Override
            public long extractAscendingTimestamp(UserStoreEvent message) {
                try {
                    return message.eventTime.toEpochSecond(ZoneOffset.of(PARTITIONS_TIMEZONE.getId()));
                } catch (Throwable t) {
                    System.out.println("alarm bad " + t.getMessage());
                }
                return 0L;
            }
        });
        return kafkaConsumer;
    }

    @Override
    protected void configStream(DataStream<UserStoreEvent> stream) {
        stream.map(new UserBehaviorTuple2Mapper()).filter((FilterFunction<Tuple2<LongWritable, Text>>) Objects::nonNull).addSink(createSink());
    }

    private SinkFunction<Tuple2<LongWritable, Text>> createSink() {
        String codecName = properties.getOrDefault("compression-codec", "snappy");
        return StreamingFileSink.forBulkFormat(
                new Path(properties.get("path")),
                new SequenceFileWriterFactory<>(configuration, LongWritable.class, Text.class, codecName)
        ).withBucketAssigner((CustomBucketAssigner<Tuple2<LongWritable, Text>>)
                (element, context) -> genDefaultBucketPath(LocalDateTime.ofInstant(Instant.ofEpochSecond(context.timestamp()), PARTITIONS_TIMEZONE))
        ).build();
    }

    private static class CustomKafkaDeserializationSchema implements KafkaDeserializationSchema<UserStoreEvent> {
        @Override
        public boolean isEndOfStream(UserStoreEvent message) {
            return false;
        }

        @Override
        public UserStoreEvent deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
            return OBJECT_MAPPER.readValue(new String(record.value(), StandardCharsets.UTF_8), UserStoreEvent.class);
        }

        @Override
        public TypeInformation<UserStoreEvent> getProducedType() {
            return TypeExtractor.getForClass(UserStoreEvent.class);
        }
    }
}
