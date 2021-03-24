package dag.service.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import dag.service.model.UserStoreEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

public class UserBehaviorTuple2Mapper implements MapFunction<UserStoreEvent, Tuple2<LongWritable, Text>> {

    private static final String DATE_TIME_FIELD = "dt";

    private static final ZoneId PARTITIONS_TIMEZONE =
            ZoneId.of("Europe/Moscow");

    private static final ObjectMapper OBJECT_MAPPER = KafkaSiteObjectMapperFactory.createObjectMapper()
            .registerModule(new KafkaSiteJacksonModule());

    @Override
    public Tuple2<LongWritable, Text> map(UserStoreEvent message) throws IOException {
        Map<String, Object> jsonForHadoopTable = new HashMap<>();
        jsonForHadoopTable.put(DATE_TIME_FIELD, message.eventTime.toEpochSecond(ZoneOffset.of(PARTITIONS_TIMEZONE.getId())));
        Map<String, Object> test = new HashMap<>();
        test.put("brand", message.brand);
        test.put("eventTime", message.eventTime);
        test.put("categoryCode", message.categoryCode);
        test.put("price", message.price);
        test.put("userId", message.userId);
        jsonForHadoopTable.putAll(test);
        return new Tuple2<>(new LongWritable(message.eventTime.toEpochSecond(ZoneOffset.of(PARTITIONS_TIMEZONE.getId()))),
                new Text(OBJECT_MAPPER.writeValueAsString(
                        OBJECT_MAPPER.readValue(OBJECT_MAPPER.writeValueAsBytes(jsonForHadoopTable), Map.class)
                )));
    }
}
