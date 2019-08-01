package com.synload.nucleo.zookeeper;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.synload.nucleo.event.NucleoResponder;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Stack;
import java.util.TreeMap;

public class ManagerImpl implements Manager{
    @Override
    public Object getServiceData(String path, boolean watchFlag) throws KeeperException, InterruptedException {
        return null;
    }

    private static ZooKeeper zkeeper;
    private static Connection connection;
    private String meshName;

    public ManagerImpl(String zkConnectionString, String meshName) {
        try {
            initialize(zkConnectionString, meshName);
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    private void initialize(String zkConnectionString, String meshName) throws IOException, InterruptedException {
        this.meshName = meshName;
        connection = new Connection();
        zkeeper = connection.connect(zkConnectionString);
        createPath("/"+this.meshName);
        createPath("/"+this.meshName+"/services");
        createPath("/"+this.meshName+"/databases");
    }

    public void closeConnection() throws Exception {
        connection.close();
    }
    public void createPath(String path) {
        try {
            zkeeper.create(
                path,
                null,
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    public List<String> getServiceList(){
        try {
            List<String> services = zkeeper.getChildren("/" + this.meshName + "/services", null);
            return services;
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }
    public List<String> getServiceNodeList(String service){
        try {
            List<String> nodes = zkeeper.getChildren("/" + this.meshName + "/services/"+service, null);
            return nodes;
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }
    public byte[] getServiceNodeInformation(String service, String node){
        try {
            byte[] data = zkeeper.getData("/" + this.meshName + "/services/"+service+"/"+node, null, null);
            return data;
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }
    public void create(String path, byte[] data)
        throws KeeperException,
        InterruptedException {

        zkeeper.create(
            path,
            data,
            ZooDefs.Ids.OPEN_ACL_UNSAFE,
            CreateMode.EPHEMERAL);
    }


    public void getServiceList(DataUpdate responder) {
        zkeeper.getChildren("/" + this.meshName + "/services", new Watcher() {
            public void process(WatchedEvent we) {
                getServiceList(responder);
            }
        }, new AsyncCallback.Children2Callback() {
            @Override
            public void processResult(int rc, String path, Object ctx, List<String> services, Stat stat) {
                responder.run(path, services);
            }
        }, null);
    }
    public void getServiceNodeList(String service, DataUpdate responder){
        zkeeper.getChildren("/" + this.meshName + "/services/"+service, null, new AsyncCallback.Children2Callback(){
            @Override
            public void processResult(int rc, String path, Object ctx, List<String> nodes, Stat stat) {
                responder.run(service, nodes);
            }
        }, null);
    }
    public static Stack<String> nodeHit = new Stack<>();
    public void getServiceNodeInformation(String service, String node, DataUpdate responder, boolean initial){
        if(initial && nodeHit.search(node)==-1){
            nodeHit.add(node);
        }else if(initial){
            return;
        }
        zkeeper.getData("/" + this.meshName + "/services/" + service + "/" + node, new Watcher() {
            public void process(WatchedEvent we) {
                getServiceNodeInformation(service, node, responder, false);
            }
        }, new AsyncCallback.DataCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                if (data != null) {
                    try {
                        responder.run(service, node, new ObjectMapper().readValue(data, ServiceInformation.class));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                } else {
                    responder.run(service, node, null);
                }
            }
        }, null);
    }

    public void update(String path, byte[] data) throws KeeperException,
        InterruptedException {
        int version = zkeeper.exists(path, true).getVersion();
        zkeeper.setData(path, data, version);
    }
}
