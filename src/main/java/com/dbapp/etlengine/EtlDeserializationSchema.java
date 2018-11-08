package com.dbapp.etlengine;

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/**
 * 描述:
 * etl
 *
 * @author lenghl
 * @create 2018-11-08 14:59
 */
public class EtlDeserializationSchema extends AbstractDeserializationSchema {

    public static final ObjectMapper objectMapper = new ObjectMapper();

    public EtlDeserializationSchema() {
        super(Model.class);
    }

    public Model deserialize(byte[] message) throws IOException {
        return objectMapper.readValue(new String(message), Model.class);
    }
}
