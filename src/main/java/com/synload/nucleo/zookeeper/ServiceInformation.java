package com.synload.nucleo.zookeeper;

import com.fasterxml.jackson.annotation.JsonRootName;

import java.io.Serializable;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;


@JsonRootName("details")
public class ServiceInformation {
    public Set<String> events;
    public String name;
    public String connectString;
    public String host;
    public String meshName;
    public String service;

    public ServiceInformation() {
    }

    public ServiceInformation(String meshName, String service, String unique, Set<String> events,  String connectString, String host) {
        this.events = events;
        this.name = unique;
        this.meshName = meshName;
        this.service = service;
        this.connectString = connectString;
        this.host = host;
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
}
