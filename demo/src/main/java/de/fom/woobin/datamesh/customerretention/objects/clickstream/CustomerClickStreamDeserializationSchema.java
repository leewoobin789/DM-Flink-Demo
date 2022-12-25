package de.fom.woobin.datamesh.customerretention.objects.clickstream;

import java.io.IOException;

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public class CustomerClickStreamDeserializationSchema
    extends AbstractDeserializationSchema<CustomerClickStream>{
    private static final long serialVersionUID = 1L;

    private transient ObjectMapper objectMapper;

    @Override
    public void open(InitializationContext context) throws Exception {
        super.open(context);
        objectMapper = JsonMapper.builder().build().registerModule(new JavaTimeModule());
    }

    @Override
    public CustomerClickStream deserialize(byte[] message) throws IOException {
        return objectMapper.readValue(message, CustomerClickStream.class);
    }
}
