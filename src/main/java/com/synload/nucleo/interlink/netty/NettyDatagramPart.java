package com.synload.nucleo.interlink.netty;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

public class NettyDatagramPart {
    private byte[] bytes;
    private String root;
    private int part;
    private int total;
    private int start;
    private long length;

    public NettyDatagramPart() {
    }

    public NettyDatagramPart(byte[] bytes, long length, int start, String root, int part, int total) {
        this.bytes = bytes;
        this.root = root;
        this.part = part;
        this.start = start;
        this.total = total;
        this.length = length;
    }

    public byte[] getBytes() {
        return bytes;
    }

    public void setBytes(byte[] bytes) {
        this.bytes = bytes;
    }

    public String getRoot() {
        return root;
    }

    public void setRoot(String root) {
        this.root = root;
    }

    public int getPart() {
        return part;
    }

    public void setPart(int part) {
        this.part = part;
    }

    public int getTotal() {
        return total;
    }

    public void setTotal(int total) {
        this.total = total;
    }

    public long getLength() {
        return length;
    }

    public void setLength(long length) {
        this.length = length;
    }

    public int getStart() {
        return start;
    }

    public void setStart(int start) {
        this.start = start;
    }
}
