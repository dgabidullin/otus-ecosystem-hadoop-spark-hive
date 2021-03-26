package dag.service;

//FIXME camelCase -> snake_case jackson, check @JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
public class UserBehaviorEvent {

    private String event_time;

    private String event_type;

    private String category_code;

    private String brand;

    private Double price;

    private Long user_id;

    public UserBehaviorEvent() {
    }

    public String getEvent_time() {
        return event_time;
    }

    public String getEvent_type() {
        return event_type;
    }

    public String getCategory_code() {
        return category_code;
    }

    public String getBrand() {
        return brand;
    }

    public Double getPrice() {
        return price;
    }

    public Long getUser_id() {
        return user_id;
    }

    public void setEvent_time(String event_time) {
        this.event_time = event_time;
    }

    public void setEvent_type(String event_type) {
        this.event_type = event_type;
    }

    public void setCategory_code(String category_code) {
        this.category_code = category_code;
    }

    public void setBrand(String brand) {
        this.brand = brand;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    public void setUser_id(Long user_id) {
        this.user_id = user_id;
    }

    enum EventType {
        VIEW,
        CART,
        REMOVE_FROM_CART,
        PURCHASE
    }

    @Override
    public String toString() {
        return "UserStoreEvent{"
                + "event_time="
                + event_time
                + ", user_id="
                + user_id
                + ", event_type='"
                + event_type
                + '\''
                + ", category_code='"
                + category_code
                + '\''
                + ", brand='"
                + brand
                + '\''
                + ", price="
                + price
                + '}';
    }
}
