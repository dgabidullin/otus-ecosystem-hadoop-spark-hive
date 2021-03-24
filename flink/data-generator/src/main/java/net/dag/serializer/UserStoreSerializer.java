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
    String csv =
        String.format(
            "%s, %s, %s, %s, %s, %s",
            ue.eventTime.format(formatter),
            ue.userId,
            ue.eventType,
            ue.categoryCode,
            ue.brand,
            ue.price);

    return csv.getBytes();
  }

  @Override
  public void close() {}
}
