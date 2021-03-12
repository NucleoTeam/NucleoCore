package com.synload.nucleo.data;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class NucleoStep implements Serializable {
    private String step="";
    private String ip;
    private String host;
    private String node;
    private long start;
    private long end;
    private long total;
    public static String hostName = null;
    public static String hostIP = "";
    public static String nodeName = "";

    public NucleoStep(NucleoStep step) {
        this.step = step.step;
        this.ip = step.ip;
        this.host = step.host;
        this.start = step.start;
        this.end = step.end;
        this.total = step.total;
        this.node = step.node;
    }

    public NucleoStep() {
        if (hostName == null) {
            try {
                InetAddress inet = InetAddress.getLocalHost();
                hostName = inet.getHostName();
                hostIP = inet.getHostAddress();
                nodeName = System.getenv("NODE_NAME");
            }catch(UnknownHostException e){
                e.printStackTrace();
            }
        }
        this.host = hostName;
        this.ip = hostIP;
        this.node = nodeName;
    }

    public NucleoStep(String step, long start) {
        this();
        this.step = step;
        this.start = start;
    }

    public long getStart() {
        return start;
    }

    public void setStart(long start) {
        this.start = start;
    }

    public long getEnd() {
        return end;
    }

    public void setEnd(long end) {
        this.end = end;
        this.total = this.end - this.start;
    }

    public long getTotal() {
        return total;
    }

    public void setTotal(long total) {
        this.total = total;
    }

    public String getStep() {
        return step;
    }

    public void setStep(String step) {
        this.step = step;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getNode() {
        return node;
    }

    public void setNode(String node) {
        this.node = node;
    }
}
