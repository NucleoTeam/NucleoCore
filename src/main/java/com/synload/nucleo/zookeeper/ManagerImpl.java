package com.synload.nucleo.zookeeper;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.synload.nucleo.NucleoMesh;
import com.synload.nucleo.event.NucleoResponder;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Stack;
import java.util.TreeMap;

public class ManagerImpl implements Manager, Runnable{
    @Override
    public Object getServiceData(String path, boolean watchFlag) throws KeeperException, InterruptedException {
        return null;
    }

    private ZooKeeper zkeeper;
    private Connection connection;
    private String connString;
    private String meshName;
    private NucleoMesh mesh;

    public ManagerImpl(String zkConnectionString, NucleoMesh mesh) {
        try {
            this.connString = zkConnectionString;
            this.meshName = mesh.getMeshName();
            this.mesh = mesh;
            connection = new Connection();
            new Thread(this).start();
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    public void run() {
        try {
            System.out.println("Connecting to Zookeeper.");
            zkeeper = connection.connect(this.connString);
            System.out.println("Registering mesh to ZK");
            createBlock("/"+this.meshName);
            System.out.println("Creating paths for services and databases to ZK");
            createBlock("/"+this.meshName+"/services");
            createBlock("/"+this.meshName+"/databases");
            System.out.println("Zookeeper connect finished.");
            mesh.zookeeperConnected();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void closeConnection() throws Exception {
        connection.close();
    }
    public void createPath(String path) {
        try {
            zkeeper.exists(path, null, new AsyncCallback.StatCallback() {
                @Override
                public void processResult(int rc, String path, Object ctx, Stat stat) {
                    try {
                        if( stat == null ) {
                            zkeeper.create(
                                path,
                                null,
                                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                                CreateMode.PERSISTENT);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }, null);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    public List<String> getServiceList(){
        try {
            List<String> services = zkeeper.getChildren("/" + this.meshName + "/services", new Watcher() {
                public void process(WatchedEvent we) {

                }
            });
            return services;
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }
    public List<String> getServiceNodeList(String service){
        try {
            List<String> nodes = zkeeper.getChildren("/" + this.meshName + "/services/"+service, new Watcher() {
                public void process(WatchedEvent we) {

                }
            });
            return nodes;
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }
    public byte[] getServiceNodeInformation(String service, String node){
        try {
            byte[] data = zkeeper.getData("/" + this.meshName + "/services/"+service+"/"+node, new Watcher() {
                public void process(WatchedEvent we) {

                }
            }, null);
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
    public void createBlock(String path) {
        try {
            zkeeper.create(
                path,
                null,
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL);
        }catch (Exception e){

        }
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
            synchronized (ManagerImpl.nodeHit) {
                nodeHit.add(node);
            }
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
