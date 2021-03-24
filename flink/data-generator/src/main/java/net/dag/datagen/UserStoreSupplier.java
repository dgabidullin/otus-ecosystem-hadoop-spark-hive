package net.dag.datagen;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;
import net.dag.model.UserStoreEvent;

public class UserStoreSupplier implements Supplier<UserStoreEvent> {

  private final Random generator = ThreadLocalRandom.current();

  private final Iterator<Long> userIds =
      Stream.generate(() -> Stream.of(1L, 2L, 3L, 4L, 5L, 6L, 7L))
          .flatMap(UnaryOperator.identity())
          .iterator();

  private final Iterator<String> eventTypes =
      Stream.generate(
              () ->
                  Stream.of(
                      UserStoreEvent.EventType.CART.name(),
                      UserStoreEvent.EventType.PURCHASE.name(),
                      UserStoreEvent.EventType.REMOVE_FROM_CART.name(),
                      UserStoreEvent.EventType.VIEW.name()))
          .flatMap(UnaryOperator.identity())
          .iterator();

  private final Iterator<String> categoryCodes =
      Stream.generate(
              () ->
                  Stream.of(
                      "electronics.smartphone",
                      "computers.notebook",
                      "electronics.audio.headphone",
                      "electronics.video.tv",
                      "electronics.clocks"))
          .flatMap(UnaryOperator.identity())
          .iterator();

  private final Iterator<String> brands =
      Stream.generate(() -> Stream.of("apple", "acer", "asus", "samsung", "huawei", "xiaomi"))
          .flatMap(UnaryOperator.identity())
          .iterator();

  private final Iterator<Double> prices =
      Stream.generate(() -> Stream.of(312.22, 3000.1, 1000.53, 550.26, 2001.10, 200.10, 300.10))
          .flatMap(UnaryOperator.identity())
          .iterator();

  private final Iterator<LocalDateTime> eventTimes =
      Stream.iterate(
              LocalDateTime.of(2021, 1, 1, 1, 0),
              time -> time.plus(generator.nextInt(1000), ChronoUnit.MILLIS))
          .iterator();

  @Override
  public UserStoreEvent get() {
    UserStoreEvent userStoreEvent = new UserStoreEvent();
    userStoreEvent.eventTime = eventTimes.next();
    userStoreEvent.eventType = eventTypes.next();
    userStoreEvent.userId = userIds.next();
    userStoreEvent.price = prices.next();
    userStoreEvent.brand = brands.next();
    userStoreEvent.categoryCode = categoryCodes.next();
    return userStoreEvent;
  }
}
