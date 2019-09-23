package com.synload.nucleo.zookeeper;

import java.util.*;

public class NodeStatus {
    public String unique;
    public String connection;
    public List<Long> pings = new ArrayList<>();
    public Date lastPing = null;

    public NodeStatus(String unique, String connection) {
        this.unique = unique;
        this.connection = connection;
    }

    public void add(Long pingTime){
        synchronized (pings) {
            if (pings.size()>20) {
                pings.remove(0);
            }
            pings.add(pingTime);
        }
        lastPing = new Date();
    }
}
