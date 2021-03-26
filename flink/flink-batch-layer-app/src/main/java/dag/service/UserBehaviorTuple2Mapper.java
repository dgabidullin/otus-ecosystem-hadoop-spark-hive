package dag.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class UserBehaviorTuple2Mapper implements MapFunction<UserBehaviorEvent, Tuple2<LongWritable, Text>> {

    private static final String DATE_TIME_FIELD = "dt";

    private static final DateTimeFormatter DEFAULT_DATE_TIME_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Override
    public Tuple2<LongWritable, Text> map(UserBehaviorEvent message) throws IOException {
        Map<String, Object> jsonForHadoopTable = new HashMap<>();
        jsonForHadoopTable.put(DATE_TIME_FIELD, LocalDateTime.parse(message.getEvent_time(), DEFAULT_DATE_TIME_FORMAT).toString());
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("event_time", message.getEvent_time());
        data.put("event_type", message.getEvent_type());
        data.put("category_code", message.getCategory_code());
        data.put("brand", message.getBrand());
        data.put("price", message.getPrice());
        data.put("user_id", message.getUser_id());

        jsonForHadoopTable.putAll(data);
        return new Tuple2<>(
                new LongWritable(
                        LocalDateTime.parse(message.getEvent_time(), DEFAULT_DATE_TIME_FORMAT).atZone(ZoneId.of("Europe/Moscow")).toEpochSecond()
                ),
                new Text(OBJECT_MAPPER.writeValueAsString(
                        OBJECT_MAPPER.readValue(OBJECT_MAPPER.writeValueAsBytes(jsonForHadoopTable), Map.class)
                )));
    }
}
