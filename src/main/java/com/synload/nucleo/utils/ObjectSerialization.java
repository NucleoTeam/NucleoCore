package com.synload.nucleo.utils;

import org.apache.commons.lang3.SerializationException;
import java.io.*;

public class ObjectSerialization {
    public byte[] serialize(Object data) throws Exception {
        if (data == null) {
            return null;
        }
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream out = null;
        try {
            out = new ObjectOutputStream(bos);
            out.writeObject(data);
            out.flush();
            return bos.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
            throw new SerializationException("Failed to serialize data", e);
        } finally {
            try {
                bos.close();
            } catch (IOException ex) {
                // ignore close exception
            }
        }
    }

    public Object deserialize(byte[] bytes) throws Exception {
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        ObjectInput in = null;
        try {
            in = new ObjectInputStream(bis);
            Object o = in.readObject();
            return o;
        } catch (IOException e) {
            e.printStackTrace();
            throw new SerializationException("Could not deserialize data", e);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw new SerializationException("Could not deserialize data", e);
        } finally {
            try {
                if (in != null) {
                    in.close();
                }
            } catch (IOException ex) {
                // ignore close exception
            }
        }
    }
}
