package net.dag;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

public class App {

    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        tableEnv.executeSql("CREATE TABLE user_behavior (" +
                "    event_time    TIMESTAMP(3)," +
                "    user_id       BIGINT," +
                "    event_type    VARCHAR(15)," +
                "    category_code VARCHAR(15)," +
                "    brand         VARCHAR(30)," +
                "    price         DOUBLE," +
                "    WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND" +
                ") WITH (" +
                "    'connector' = 'kafka'," +
                "    'topic'     = 'user_behavior'," +
                "    'properties.bootstrap.servers' = 'kafka:9092'," +
                "    'format'    = 'json'" +
                ")");

        tableEnv.executeSql("CREATE TABLE buy_cnt_per_hour (" +
                "    hour_of_day    BIGINT," +
                "    buy_cnt        BIGINT" +
                ") WITH (" +
                "    'connector' = 'elasticsearch-7'," +
                "    'hosts' = 'http://elasticsearch:9200'," +
                "    'index' = 'buy_cnt_per_hour'" +
                ")"
        );

        Table data = tableEnv.from("user_behavior");

        tableEnv.sqlQuery("SELECT HOUR(TUMBLE_START(event_time, INTERVAL '1' HOUR)), COUNT(*)" +
                        "  FROM " + data +
                        "  WHERE event_type = 'PURCHASE'" +
                        "  GROUP BY TUMBLE(event_time, INTERVAL '1' HOUR)"
        ).executeInsert("buy_cnt_per_hour");
    }
}
