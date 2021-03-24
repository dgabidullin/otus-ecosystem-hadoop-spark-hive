package dag.service.impl;

import dag.service.KafkaProcessingService;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.time.LocalDateTime;
import java.util.*;

import static org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer.INSTANCE;

abstract public class KafkaProcessor<T> implements KafkaProcessingService {

    private static final String KAFKA_PROPERTIES_SEPARATOR = ",";

    private static final String KAFKA_PROPERTIES_DELIMITER = ":";
    protected Map<String, String> properties;

    protected static final List<String> REQUIRED_PROPERTIES = new ArrayList<>();

    static {
        REQUIRED_PROPERTIES.add("topic");
        REQUIRED_PROPERTIES.add("path");
    }

    protected void process(String[] args) throws Exception {
        properties = getParams(ParameterTool.fromArgs(args));
        FlinkKafkaConsumer<T> kafkaConsumer = createKafkaConsumer(properties.get("topic"), getKafkaProperties());
        StreamExecutionEnvironment env = getStreamExecutionEnvironment();
        DataStream<T> stream = env.addSource(kafkaConsumer).name("KafkaSource");
        configStream(stream);
        env.execute(properties.getOrDefault("name", "Flink Streaming Job"));
    }

    protected Map<String, String> getParams(ParameterTool params) {
        try {
            //REQUIRED_PROPERTIES.forEach(params::getRequired);
            Map<String, String> param = new HashMap<>();
            param.put("topic", "user_behavior");
            param.put("path", "hdfs://data/user_behaviour");
            param.put("broker", "kafka:9092");

            return param;
        } catch (Exception e) {
            System.out.println("Usage: flink run [options] --topic <argument> --path <argument>\n" +
                    "    options:\n" +
                    "        --name                 <string>           Name of job\n" +
                    "        --broker               <string,..>        Bootstrap broker(s) (host[:port], default: localhost:9092)\n" +
                    "        --group                <string>           Kafka consumer group name (default: flink)\n" +
                    "        --topic                <string>           Topic to consume from\n" +
                    "        --kafka-properties     <string=any,...>   Additional consumer properties (security.protocol,...)\n" +
                    "        --path                 <string>           Base path where we want to write our data\n" +
                    "        --checkpoints-path     <string>           Directory used for storing the meta data of checkpoints (default: ${path}/.checkpoints)\n" +
                    "        --checkpoints-interval <int>              Start a checkpoint every <interval> (default: 60s)\n\n" +
                    "        --compression-codec    <string>           Compression codec\n\n" +
                    "Error: " + e.getMessage() + "\n");
            throw new RuntimeException(e);
        }
    }

    protected Properties getKafkaProperties() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", properties.getOrDefault("broker", "localhost:9092"));
        props.setProperty("group.id", properties.getOrDefault("group", "flink"));

        String kafkaProperties = properties.get("kafka-properties");
        if (StringUtils.isNotBlank(kafkaProperties)) {
            String[] pairs = kafkaProperties.split(KAFKA_PROPERTIES_SEPARATOR);
            Arrays.stream(pairs).map(pair -> pair.split(KAFKA_PROPERTIES_DELIMITER)).forEach(keyValue -> props.setProperty(keyValue[0], keyValue[1]));
        }
        return props;
    }

    protected StreamExecutionEnvironment getStreamExecutionEnvironment() {
        long checkpointsInterval = Long.parseLong(properties.getOrDefault("checkpoints-interval", "60"));
        String checkpointsPath = properties.getOrDefault("checkpoints-path", new Path(properties.get("path"), ".checkpoints").toString());
        return CreateStreamExecutionEnvironment(checkpointsInterval, checkpointsPath);
    }

    protected abstract FlinkKafkaConsumer<T> createKafkaConsumer(String topicName, Properties kafkaProperties);

    protected abstract void configStream(DataStream<T> stream);

    public static String genDefaultBucketPath(LocalDateTime dateTime) {
        return genDefaultBucketPath(dateTime.getYear(), dateTime.getMonth().getValue(), dateTime.getDayOfMonth());
    }

    public static String genDefaultBucketPath(int year, int month, int dayOfMonth) {
        return String.format("year=%d/month=%02d/day=%02d", year, month, dayOfMonth);
    }

    public interface CustomBucketAssigner<F> extends BucketAssigner<F, String> {
        @Override
        default SimpleVersionedSerializer<String> getSerializer() { return INSTANCE; }
    }

    @Override
    public StreamExecutionEnvironment CreateStreamExecutionEnvironment(long checkpointInterval, String checkpointPath) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        environment.enableCheckpointing(checkpointInterval * 1000, CheckpointingMode.EXACTLY_ONCE);
        environment.setStateBackend((StateBackend) new FsStateBackend(checkpointPath));
        environment.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
        return environment;
    }
}
