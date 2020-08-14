package com.synload.nucleo.zookeeper;

import org.apache.curator.shaded.com.google.common.collect.Sets;

import java.util.*;

public class NodeStatus {
    public String unique;
    public String connection;
    public List<Long> pings = new ArrayList<>();
    public Date lastPing = new Date();
    public List<String> route;

    public NodeStatus(String unique, String connection, List<String> route) {
        this.unique = unique;
        this.connection = connection;
        this.route = route;
    }

    public void add(Long pingTime){
        synchronized (pings) {
            if (pings.size()>10) {
                pings.remove(0);
            }
            pings.add(pingTime);
        }
        lastPing = new Date();
    }
}
