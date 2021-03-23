package com.synload.nucleo.interlink;

import com.synload.nucleo.data.NucleoData;
import com.synload.nucleo.utils.ObjectSerialization;
import org.apache.commons.lang3.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.zip.GZIPInputStream;

public class ObjectDeserializer implements Deserializer<NucleoData> {
    private static ObjectSerialization serializer = new ObjectSerialization();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public NucleoData deserialize(String topic, byte[] data) {
        try {
            return (NucleoData) serializer.deserialize(data);
        } catch (Exception e) {
            e.printStackTrace();
            throw new SerializationException("Could not deserialize data", e);
        }
    }

    @Override
    public NucleoData deserialize(String topic, Headers headers, byte[] data) {
        return deserialize(topic, data);
    }

    @Override
    public void close() {

    }
    byte[] decompress(byte[] data) throws IOException {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data);
        GZIPInputStream gzipInputStream = null;
        try {
            gzipInputStream = new GZIPInputStream(byteArrayInputStream);
            return gzipInputStream.readAllBytes();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            byteArrayInputStream.close();
            gzipInputStream.close();
        }
        return null;
    }
}
