package net.dag;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

public class App {

    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);
        tEnv.executeSql("CREATE TABLE user_behavior (\n" +
                "    eventTime    TIMESTAMP(3),\n" +
                "    userId       VARCHAR(50),\n" +
                "    eventType    VARCHAR(50),\n" +
                "    categoryCode VARCHAR(50),\n" +
                "    brand        VARCHAR(50),\n" +
                "    price        DOUBLE,\n" +
                "    WATERMARK FOR eventTime AS eventTime - INTERVAL '10' SECOND\n" +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic'     = 'user_behavior',\n" +
                "    'properties.bootstrap.servers' = 'kafka:9092',\n" +
                "    'format'    = 'csv'\n" +
                ")");

        tEnv.executeSql("CREATE TABLE buy_cnt_per_hour (\n" +
                "    hour_of_day    BIGINT,\n" +
                "    buy_cnt        BIGINT\n" +
                ") WITH (\n" +
                "    'connector' = 'elasticsearch-7',\n" +
                "    'hosts' = 'http://elasticsearch:9200',\n" +
                "    'index' = 'buy_cnt_per_hour'\n" +
                ")");
//
//        tEnv.executeSql("CREATE TABLE buy_cnt_per_hour_mart (\n" +
//                "    hour_of_day    BIGINT,\n" +
//                "    buy_cnt        BIGINT\n" +
//                ") WITH (\n" +
//                "  'connector'  = 'jdbc',\n" +
//                "  'url'        = 'jdbc:mysql://mysql:3306/sql-demo',\n" +
//                "  'table-name' = 'buy_cnt_per_hour_mart',\n" +
//                "  'driver'     = 'com.mysql.jdbc.Driver',\n" +
//                "  'username'   = 'sql-demo',\n" +
//                "  'password'   = 'demo-sql'\n" +
//                ")");
//
//
        Table data =
                tEnv.from("user_behavior");

        tEnv.sqlQuery(
                "    SELECT HOUR(TUMBLE_START(eventTime, INTERVAL '1' HOUR)), COUNT(*)\n" +
                        "    FROM " + data +
                        "    WHERE eventType = 'PURCHASE'     \n" +
                        "    GROUP BY TUMBLE(eventTime, INTERVAL '1' HOUR)"
        ).executeInsert("buy_cnt_per_hour_mart");
    }
}
