package de.fom.woobin.datamesh;

import java.util.Properties;

import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import de.fom.woobin.datamesh.customerretention.objects.clickstream.CustomerClickStream;
import de.fom.woobin.datamesh.customerretention.objects.clickstream.CustomerClickStreamSerializationSchema;
import de.fom.woobin.datamesh.customerretention.sources.CustomerClickStreamGenerator;
import de.fom.woobin.datamesh.inventory.objects.stock.ItemStock;
import de.fom.woobin.datamesh.inventory.objects.stock.ItemStockSerializationSchema;
import de.fom.woobin.datamesh.inventory.sources.ItemStockGenerator;

public class IntialIngestion {
    
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000L);
        env.setParallelism(2);

        produceCustomerClickStream(env);
        produceItemStock(env);

        env.execute("KafkaProducerJob");
    }

    static private void produceCustomerClickStream(StreamExecutionEnvironment env) {
        Properties kafkaProps = new Properties();
        kafkaProps.put("transaction.timeout.ms", 600000);

        KafkaSink<CustomerClickStream> sink =
                KafkaSink.<CustomerClickStream>builder()
                        .setBootstrapServers(Constants.BROKER)
                        .setKafkaProducerConfig(kafkaProps)
                        .setRecordSerializer(new CustomerClickStreamSerializationSchema(Constants.CustomerRetention.CUSTOMER_CLICK_STREAM_TOPIC))
                        .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                        .setTransactionalIdPrefix("customerclickstream-producer")
                        .build();

        env.addSource(new CustomerClickStreamGenerator()).sinkTo(sink);
    }

    static private void produceItemStock(StreamExecutionEnvironment env) {
        Properties kafkaProps = new Properties();
        kafkaProps.put("transaction.timeout.ms", 600000);

        KafkaSink<ItemStock> sink =
                KafkaSink.<ItemStock>builder()
                        .setBootstrapServers(Constants.BROKER)
                        .setKafkaProducerConfig(kafkaProps)
                        .setRecordSerializer(new ItemStockSerializationSchema(Constants.Inventory.ITEM_STOCK_TOPIC))
                        .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                        .setTransactionalIdPrefix("itemstock-producer")
                        .build();

        env.addSource(new ItemStockGenerator()).sinkTo(sink);
    }
}
