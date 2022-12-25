package de.fom.woobin.datamesh;

import java.time.ZoneId;
import java.util.Properties;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ClickStreamStockItemAggregation {
    
	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
		tEnv.getConfig().getConfiguration().setString("table.exec.source.idle-timeout", "100");
    tEnv.getConfig().setLocalTimeZone(ZoneId.of("Europe/Berlin"));

		Properties kafkaProps = new Properties();
        kafkaProps.put("transaction.timeout.ms", 600000);

        //################################################
        // Source: clickstream_aggregate
        //################################################
        tEnv.executeSql(String.format(
            "CREATE TABLE clickstream_aggregate (\n" +
            "    itemId STRING NOT NULL,\n" +
            "    aggregationEnd TIMESTAMP(3),\n" +
            "    countStreamAggregate     BIGINT,\n" +
            "    WATERMARK FOR aggregationEnd AS aggregationEnd - INTERVAL '10' SECOND,\n" +
            "    PRIMARY KEY(itemId) NOT ENFORCED\n" +
            ") WITH (\n" +
            "    'connector' = 'upsert-kafka',\n" +
            "    'topic'     = '%s',\n" +
            "    'properties.bootstrap.servers' = 'localhost:9092',\n" +
            "    'properties.group.id' = '%s',\n" +
            "    'key.format'    = 'raw',\n" +
            "    'value.format'    = 'json'\n" +
            ")",
            Constants.CustomerRetention.CUSTOMER_CLICK_STREAM_AGGREGATE_TOPIC,
            Constants.Inventory.CONSUMER_GROUP
        ));
        
        //################################################
        // Source: item_stock
        //################################################
        tEnv.executeSql(String.format(
            "CREATE TABLE item_stock (\n" +
            "    id STRING NOT NULL,\n" +
            "    itemName     STRING NOT NULL,\n" +
            "    stock     BIGINT,\n" +
            "    updatedTs     STRING,\n" +
            "    newUpdatedTs  AS TO_TIMESTAMP(updatedTs),\n" +
            "    WATERMARK FOR newUpdatedTs AS newUpdatedTs - INTERVAL '10' SECOND,\n" +
            "    PRIMARY KEY(id) NOT ENFORCED\n" +
            ") WITH (\n" +
            "    'connector' = 'upsert-kafka',\n" +
            "    'topic'     = '%s',\n" +
            "    'properties.bootstrap.servers' = 'localhost:9092',\n" +
            "    'properties.group.id' = '%s',\n" +
            "    'key.format'    = 'raw',\n" +
            "    'value.format'    = 'json'\n" +
            ")", 
            Constants.Inventory.ITEM_STOCK_TOPIC,
            Constants.Inventory.CONSUMER_GROUP
        ));

        //tEnv.sqlQuery("SELECT * FROM item_stock").execute().print();

        //################################################
        // Sink: itemstock_clickstream
        //################################################      
        
        tEnv.executeSql(String.format(
            "CREATE TABLE itemstock_clickstream (\n" +
            "    id STRING NOT NULL,\n" +
            "    itemName     STRING NOT NULL,\n" +
            "    stock     BIGINT,\n" +
            "    aggregationEnd     TIMESTAMP(3),\n" +
            "    countStreamAggregate     BIGINT,\n" +
            "    PRIMARY KEY(id) NOT ENFORCED\n" +
            ") WITH (\n" +
            "    'connector' = 'upsert-kafka',\n" +
            "    'topic'     = '%s',\n" +
            "    'properties.bootstrap.servers' = 'localhost:9092',\n" +
            "    'key.format' = 'raw',\n" +
            "    'value.format' = 'json'\n" +
            ")", 
            Constants.Inventory.ITEM_STOCK_CLICK_STREAM_TOPIC
        ));

        //################################################
        // join
        //################################################
        
        Table enrichedRecords =
                tEnv.sqlQuery(
                  "SELECT\n" +
                    "item_stock.id AS id,\n" +
                    "item_stock.itemName AS itemName,\n" +
                    "item_stock.stock AS stock,\n" +
                    "clickstream_aggregate.aggregationEnd AS aggregationEnd,\n" +
                    "clickstream_aggregate.countStreamAggregate AS countStreamAggregate\n" +
                  "FROM item_stock\n" +
                  "LEFT JOIN clickstream_aggregate FOR SYSTEM_TIME AS OF item_stock.newUpdatedTs\n" +
                  "ON item_stock.id = clickstream_aggregate.itemId"
                );

        enrichedRecords.executeInsert("itemstock_clickstream");
        
	}
}
