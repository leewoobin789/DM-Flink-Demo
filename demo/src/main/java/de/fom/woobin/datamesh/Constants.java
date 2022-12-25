package de.fom.woobin.datamesh;

public class Constants {
    public static String BROKER = "localhost:9092";
    
    
    static class Inventory {
        public static String CONSUMER_GROUP = "de.fom.inventory.datamesh";
        public static String ITEM_STOCK_TOPIC = "de.fom.inventory.itemstock";
        public static String ITEM_STOCK_CLICK_STREAM_TOPIC = "de.fom.inventory.itemstock_clickstream";
    }
    static class CustomerRetention {
        public static String CONSUMER_GROUP = "de.fom.customerretention.datamesh";
        public static String CUSTOMER_CLICK_STREAM_TOPIC = "de.fom.customerretention.clickstream";
        public static String CUSTOMER_CLICK_STREAM_AGGREGATE_TOPIC = "de.fom.customerretention.clickstream_aggregate";
    }
}
