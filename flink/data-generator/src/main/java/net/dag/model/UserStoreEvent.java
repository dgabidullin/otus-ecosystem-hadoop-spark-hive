package net.dag.model;

import java.time.LocalDateTime;

public class UserStoreEvent {

  public LocalDateTime eventTime;

  public String eventType;

  public String categoryCode;

  public String brand;

  public Double price;

  public Long userId;

  public enum EventType {
    VIEW,
    CART,
    REMOVE_FROM_CART,
    PURCHASE
  }

  @Override
  public String toString() {
    return "UserStoreEvent{"
        + "eventTime="
        + eventTime
        + ", userId="
        + userId
        + ", eventType='"
        + eventType
        + '\''
        + ", categoryCode='"
        + categoryCode
        + '\''
        + ", brand='"
        + brand
        + '\''
        + ", price="
        + price
        + '}';
  }
}
