package com.synload.nucleo.zookeeper;

import org.apache.zookeeper.KeeperException;

import java.util.List;

public interface Manager {
     void create(String path, byte[] data) throws KeeperException, InterruptedException;
     Object getServiceData(String path, boolean watchFlag) throws KeeperException, InterruptedException;
     List<String> getServiceList();
     List<String> getServiceNodeList(String service);
     void getServiceList(DataUpdate responder);
     void getServiceNodeList(String service, DataUpdate responder);
     void getServiceNodeInformation(String service, String node, DataUpdate responder, boolean initial);
     byte[] getServiceNodeInformation(String service, String node);
     void update(String path, byte[] data) throws KeeperException, InterruptedException;
     void createPath(String path) throws KeeperException, InterruptedException;
}
