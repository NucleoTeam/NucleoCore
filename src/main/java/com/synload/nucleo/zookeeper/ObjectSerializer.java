package com.synload.nucleo.zookeeper;

import com.synload.nucleo.utils.ObjectSerialization;
import java.io.*;
import java.util.Base64;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class ObjectSerializer {
    private static ObjectSerialization serializer = new ObjectSerialization();

    public String serialize(ServiceInformation instance) throws Exception {
        return Base64.getEncoder().encodeToString(
            compress(serializer.serialize(instance))
        );
    }

    public ServiceInformation deserialize(String bytes) throws Exception {
        return (ServiceInformation) serializer.deserialize(
            decompress(Base64.getDecoder().decode(
                bytes
            ))
        );
    }

    byte[] compress(byte[] data) throws IOException {
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream(data.length);
        try {
            GZIPOutputStream zipStream = new GZIPOutputStream(byteStream);
            try {
                zipStream.write(data);
            } finally {
                zipStream.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            byteStream.close();
        }
        return byteStream.toByteArray();
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
