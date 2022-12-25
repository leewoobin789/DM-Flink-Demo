package de.fom.woobin.datamesh.inventory.objects.stock;

import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public class ItemStockSerializationSchema 
    implements KafkaRecordSerializationSchema<ItemStock>{
    private static final long serialVersionUID = 1L;

    private String topic;
    private static final ObjectMapper objectMapper =
            JsonMapper.builder()
                    .build()
                    .registerModule(new JavaTimeModule())
                    .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

    public ItemStockSerializationSchema() {}

    public ItemStockSerializationSchema(String topic) {
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(
        ItemStock element, KafkaSinkContext context, Long timestamp) {

        try {
            return new ProducerRecord<>(
                    topic,
                    null,
                    System.currentTimeMillis(),
                    element.id.getBytes(),
                    objectMapper.writeValueAsBytes(element));
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Could not serialize record: " + element, e);
        }
    }
}
