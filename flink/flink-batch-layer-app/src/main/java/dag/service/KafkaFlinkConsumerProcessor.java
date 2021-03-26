package dag.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.formats.sequencefile.SequenceFileWriterFactory;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.charset.StandardCharsets;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer.INSTANCE;

public class KafkaFlinkConsumerProcessor {

    private static final String HDFS_DATA_PATH = "hdfs://namenode:8020/data/user_behavior";

    private static final DateTimeFormatter DEFAULT_DATE_TIME_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private static final String ZONE_ID_MOSCOW  = "Europe/Moscow";

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        KafkaFlinkConsumerProcessor processor = new KafkaFlinkConsumerProcessor();
        StreamExecutionEnvironment env = processor.createStreamExecutionEnvironment(HDFS_DATA_PATH);

        FlinkKafkaConsumer<UserBehaviorEvent> kafkaSource = new FlinkKafkaConsumer<>(
                "user_behavior",
                new CustomKafkaDeserializationSchema(),
                processor.getKafkaProperties()
        );
        kafkaSource.assignTimestampsAndWatermarks(WatermarkStrategy.<UserBehaviorEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner((event, ts) -> LocalDateTime.parse(event.getEvent_time(), DEFAULT_DATE_TIME_FORMAT).atZone(ZoneId.of(ZONE_ID_MOSCOW)).toEpochSecond()));

        env.addSource(kafkaSource)
                .name("KafkaSource")
                .map(new UserBehaviorTuple2Mapper())
                .filter((FilterFunction<Tuple2<LongWritable, Text>>) Objects::nonNull)
                .addSink(processor.createSink());
        env.execute("FlinkApp");
    }

    public Properties getKafkaProperties() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "FlinkConsumerGroup");
        return props;
    }

    public StreamExecutionEnvironment createStreamExecutionEnvironment(String checkpointPath) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.enableCheckpointing(Duration.ofSeconds(60).toMillis());
        environment.setStateBackend(new FsStateBackend(checkpointPath));
        return environment;
    }

    private SinkFunction<Tuple2<LongWritable, Text>> createSink() {
        return StreamingFileSink.forBulkFormat(
                new Path(HDFS_DATA_PATH),
                new SequenceFileWriterFactory<>(new Configuration(), LongWritable.class, Text.class) // fixme need some codec
        ).withBucketAssigner((CustomBucketAssigner<Tuple2<LongWritable, Text>>)
                (element, context) -> genDefaultBucketPath(
                        LocalDateTime.ofInstant(Instant.ofEpochSecond(context.timestamp()), ZoneId.of(ZONE_ID_MOSCOW))
                )
        ).build();
    }

    private static String genDefaultBucketPath(LocalDateTime dateTime) {
        return genDefaultBucketPath(dateTime.getYear(), dateTime.getMonth().getValue(), dateTime.getDayOfMonth());
    }

    private static String genDefaultBucketPath(int year, int month, int dayOfMonth) {
        return String.format("year=%d/month=%02d/day=%02d", year, month, dayOfMonth);
    }

    private static class CustomKafkaDeserializationSchema implements KafkaDeserializationSchema<UserBehaviorEvent> {
        @Override
        public boolean isEndOfStream(UserBehaviorEvent message) {
            return false;
        }

        @Override
        public UserBehaviorEvent deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
            return OBJECT_MAPPER.readValue(new String(record.value(), StandardCharsets.UTF_8), UserBehaviorEvent.class);
        }

        @Override
        public TypeInformation<UserBehaviorEvent> getProducedType() {
            return TypeExtractor.getForClass(UserBehaviorEvent.class);
        }
    }

    private interface CustomBucketAssigner<F> extends BucketAssigner<F, String> {
        @Override
        default SimpleVersionedSerializer<String> getSerializer() {
            return INSTANCE;
        }
    }
}
