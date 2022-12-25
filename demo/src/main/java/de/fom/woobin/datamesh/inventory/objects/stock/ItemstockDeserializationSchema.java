package de.fom.woobin.datamesh.inventory.objects.stock;

import java.io.IOException;

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public class ItemstockDeserializationSchema 
    extends AbstractDeserializationSchema<ItemStock>{
    private static final long serialVersionUID = 1L;

    private transient ObjectMapper objectMapper;

    @Override
    public void open(InitializationContext context) throws Exception {
        super.open(context);
        objectMapper = JsonMapper.builder().build().registerModule(new JavaTimeModule());
    }

    @Override
    public ItemStock deserialize(byte[] message) throws IOException {
        return objectMapper.readValue(message, ItemStock.class);
    }
}
