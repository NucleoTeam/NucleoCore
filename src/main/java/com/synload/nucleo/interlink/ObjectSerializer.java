package com.synload.nucleo.interlink;

import com.synload.nucleo.data.NucleoData;
import com.synload.nucleo.utils.ObjectSerialization;
import org.apache.commons.lang3.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class ObjectSerializer implements Serializer<NucleoData> {
    private static ObjectSerialization serializer = new ObjectSerialization();
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, NucleoData data) {
        if (data == null) {
            return null;
        }
        try {
            return serializer.serialize(data);
        } catch (Exception e) {
            e.printStackTrace();
            throw new SerializationException("Failed to serialize data", e);
        }
    }

    @Override
    public byte[] serialize(String topic, Headers headers, NucleoData data) {
        return serialize(topic, data);
    }

    @Override
    public void close() {

    }
}
