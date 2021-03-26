package net.dag.serializer;

import java.time.format.DateTimeFormatter;
import java.util.Map;
import net.dag.model.UserStoreEvent;
import org.apache.kafka.common.serialization.Serializer;

public class UserStoreSerializer implements Serializer<UserStoreEvent> {

  private static final DateTimeFormatter formatter =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

  @Override
  public void configure(Map<String, ?> map, boolean b) {}

  @Override
  public byte[] serialize(String s, UserStoreEvent ue) {
    String json =
        String.format(
            "{\"event_time\": \"%s\", \"user_id\": %s, \"event_type\": \"%s\", \"category_code\": \"%s\", \"brand\": \"%s\", \"price\": %s}",
            ue.eventTime.format(formatter),
            ue.userId,
            ue.eventType,
            ue.categoryCode,
            ue.brand,
            ue.price);

    return json.getBytes();
  }

  @Override
  public void close() {}
}
