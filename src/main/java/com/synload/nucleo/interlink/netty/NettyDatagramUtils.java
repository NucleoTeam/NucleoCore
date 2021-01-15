package com.synload.nucleo.interlink.netty;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.synload.nucleo.hub.Hub;
import com.synload.nucleo.interlink.InterlinkHandler;
import com.synload.nucleo.interlink.InterlinkMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.*;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

public class NettyDatagramUtils {
    private class Combiner {
        private int parts = -1;
        private byte[] array;

        public Combiner(int length) {
            this.array = new byte[length];
        }

        public int getParts() {
            return parts;
        }

        public void setParts(int parts) {
            this.parts = parts;
        }

        public byte[] getArray() {
            return array;
        }

        public void setArray(byte[] array) {
            this.array = array;
        }

        public void combine(NettyDatagramPart part) {
            int i = 0;
            for (int x = part.getStart(); x < part.getStart() + part.getBytes().length; x++) {
                this.array[x] = part.getBytes()[i];
                i++;
            }
            parts++;
        }
    }

    public Map<String, Combiner> buffer = Maps.newHashMap();

    @JsonIgnore
    private static ObjectMapper mapper = new ObjectMapper() {{
        this.enableDefaultTyping();
        this.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }};

    public static byte[] compress(byte[] data) {
        Deflater deflater = new Deflater();
        deflater.setLevel(Deflater.BEST_COMPRESSION);
        deflater.setInput(data);
        deflater.finish();

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        byte[] buffer = new byte[4096];
        while (!deflater.finished()) {
            int count = deflater.deflate(buffer); // returns the generated code... index
            outputStream.write(buffer, 0, count);
        }
        byte[] array = outputStream.toByteArray();
        try {
            outputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return array;
    }

    public static byte[] decompress(byte[] data) {
        Inflater inflater = new Inflater();
        inflater.setInput(data);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        byte[] buffer = new byte[4096];
        while (!inflater.finished()) {
            try {
                int count = inflater.inflate(buffer);
                outputStream.write(buffer, 0, count);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        byte[] array = outputStream.toByteArray();
        try {
            outputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return array;
    }

    protected static final Logger logger = LoggerFactory.getLogger(NettyDatagramUtils.class);

    private InterlinkHandler interlinkHandler;
    public NettyDatagramUtils(InterlinkHandler interlinkHandler) {
        this.interlinkHandler = interlinkHandler;
    }

    int x = 0;

    public void send(DatagramSocket socket, InterlinkMessage interlinkMessage, InetAddress address, int port) {
        try {
            byte[] compressed = mapper.writeValueAsBytes(interlinkMessage);
            //logger.info((x++) + ": sent size: " + compressed.length);
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(compressed);
            int length = byteArrayInputStream.available();
            int left;
            int start = 0;
            int size = 1000;
            int part = 0;
            int total = (int) Math.ceil(length / size);
            byte[] buffer = new byte[size];
            String uuid = UUID.randomUUID().toString();
            while ((left = byteArrayInputStream.available()) > 0) {
                if (size > left) {
                    buffer = new byte[left];
                    size = left;
                }
                try {
                    byteArrayInputStream.read(buffer);
                    byte[] nettyDatagramPart = mapper.writeValueAsBytes(new NettyDatagramPart(buffer, length, start, uuid, part, total));
                    byte[] array = compress(nettyDatagramPart);
                    socket.send(new DatagramPacket(array, array.length, address, port));
                } catch (Exception e) {
                    e.printStackTrace();
                }
                //logger.info((x) + ": " + part);
                start += size;
                part++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void allReceived(byte[] bytes) {
        //logger.info((x) + ": received size: " + bytes.length);
        try {
            InterlinkMessage interlinkMessage = mapper.readValue(bytes, InterlinkMessage.class);
            logger.info("Message for topic " + interlinkMessage.getTopic());
            interlinkHandler.handleMessage(interlinkMessage.getTopic(), interlinkMessage.getData());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void receivePacket(byte[] datagramPartBytes) {
        //logger.info("size: "+datagramPartBytes.length);
        try {
            NettyDatagramPart nettyDatagramPart = mapper.readValue(decompress(datagramPartBytes), NettyDatagramPart.class);
            //logger.info((x) + "= " +
            //    "received part= " + nettyDatagramPart.getPart() + " of " + nettyDatagramPart.getTotal() + " start= " +nettyDatagramPart.getStart() + "len= "+nettyDatagramPart.getLength());
            String key = nettyDatagramPart.getRoot();
            if (!buffer.containsKey(key))
                buffer.put(key, new Combiner((int) nettyDatagramPart.getLength()));
            Combiner combiner = buffer.get(key);
            combiner.combine(nettyDatagramPart);
            if (combiner.getParts() == nettyDatagramPart.getTotal()) {
                //logger.info(new String(combiner.getArray()));
                allReceived(combiner.getArray());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
