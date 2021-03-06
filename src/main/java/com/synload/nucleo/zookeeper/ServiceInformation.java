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
    public String uniqueName;
    public String meshName;
    public String service;

    public ServiceInformation() {
    }

    public ServiceInformation(String meshName, String service, String uniqueName, Set<String> events, boolean leader) {
        this.events = events;
        this.leader = leader;
        this.uniqueName = uniqueName;
        this.meshName = meshName;
        this.service = service;
    }

    public Set<String> getEvents() {
        return events;
    }

    public void setEvents(Set<String> events) {
        this.events = events;
    }

    public String getUniqueName() {
        return uniqueName;
    }

    public void setUniqueName(String uniqueName) {
        this.uniqueName = uniqueName;
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
