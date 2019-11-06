package com.synload.nucleo.zookeeper;

import com.fasterxml.jackson.annotation.JsonRootName;

import java.io.Serializable;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;


@JsonRootName("details")
public class ServiceInformation {
    public Set<String> events;
    public boolean leader;
    public String name;
    public String connectString;
    public String host;
    public String meshName;
    public String service;

    public ServiceInformation() {
    }

    public ServiceInformation(String meshName, String service, String name, Set<String> events,  String connectString, String host, boolean leader) {
        this.events = events;
        this.leader = leader;
        this.name = name;
        this.connectString = connectString;
        this.host = host;
        this.meshName = meshName;
        this.service = service;
    }

    public Set<String> getEvents() {
        return events;
    }

    public void setEvents(Set<String> events) {
        this.events = events;
    }

    public String getName() {
        return name;
    }

    public void setName(String unique) {
        this.name = unique;
    }

    public String getConnectString() {
        return connectString;
    }

    public void setConnectString(String connectString) {
        this.connectString = connectString;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getMeshName() {
        return meshName;
    }

    public void setMeshName(String meshName) {
        this.meshName = meshName;
    }

    public String getService() {
        return service;
    }

    public void setService(String service) {
        this.service = service;
    }

    public boolean isLeader() {
        return leader;
    }

    public void setLeader(boolean leader) {
        this.leader = leader;
    }
}
