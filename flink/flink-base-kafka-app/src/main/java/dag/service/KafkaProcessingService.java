package dag.service;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public interface KafkaProcessingService {
    StreamExecutionEnvironment CreateStreamExecutionEnvironment(long checkpointInterval, String checkpointPath);
}
