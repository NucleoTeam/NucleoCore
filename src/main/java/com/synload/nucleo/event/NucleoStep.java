package com.synload.nucleo.event;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class NucleoStep {
    private String step="";
    private String ip;
    private String host;
    private long start;
    private long end;
    private long total;
    public static String hostName = null;
    public static String hostIP = "";

    public NucleoStep() {
        if (hostName == null) {
            try {
                hostName = InetAddress.getLocalHost().getHostName();
                hostIP = InetAddress.getLocalHost().getHostAddress();
            }catch(UnknownHostException e){
                e.printStackTrace();
            }
        }
        this.host = hostName;
        this.ip = hostIP;
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
}
