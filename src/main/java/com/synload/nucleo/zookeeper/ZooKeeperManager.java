package com.synload.nucleo.zookeeper;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.synload.nucleo.NucleoMesh;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Stack;

public class ZooKeeperManager implements Runnable{

    private CuratorFramework zooClient;
    private Connection connection;
    private String connString;
    private String meshName;
    private NucleoMesh mesh;

    public ZooKeeperManager(String zkConnectionString, NucleoMesh mesh) {
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
            zooClient = connection.connect(this.connString);
            System.out.println("UpSet service "+mesh.getServiceName()+" to zookeeper");
            create("/" + meshName, ( _____, __ )->
                create("/" + meshName + "/services", ( ___, ____ ) ->
                    create("/" + meshName + "/services/" + mesh.getServiceName(), (client, event) -> {
                        System.out.println("=======================================================");
                        System.out.println("/" + meshName + "/services/" + mesh.getServiceName());
                        System.out.println(KeeperException.Code.get(event.getResultCode()));
                        mesh.zookeeperConnected();
                        System.out.println("Starting zookeeper sync");
                        new Thread(new SyncList()).start();
                        new Thread(new WatchNodeList()).start();
                    })
                )
            );
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    public void delete(String path, BackgroundCallback callback){
        try {
            zooClient.delete().inBackground(callback).forPath(path);
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    public void create(String path, BackgroundCallback callback){
        try {
            zooClient.create().withMode(CreateMode.PERSISTENT).inBackground(callback).forPath(path);
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    public void closeConnection() throws Exception {
        connection.close();
    }

    public void getServiceList(DataUpdate responder) {
        try {
            zooClient.getChildren().watched().inBackground((CuratorFramework client, CuratorEvent event)->{
                List<String> children = event.getChildren();
                responder.run("/" + this.meshName + "/services", children);
            }).forPath("/" + this.meshName + "/services");


        }catch (Exception e){
            e.printStackTrace();
        }
    }
    public void getServiceNodeList(String service, DataUpdate responder){
        try {
            zooClient.getChildren().watched().inBackground((CuratorFramework client, CuratorEvent event)->{
                List<String> children = event.getChildren();
                responder.run(service, children);
            }).forPath("/" + this.meshName + "/services/"+service);

        }catch (Exception e){
            e.printStackTrace();
        }
    }
    public void getServiceNodeInformation(String service, String node, DataUpdate responder, boolean initial){
        try {
            zooClient.getData().watched().inBackground((CuratorFramework client, CuratorEvent event)->{
                byte[] data = event.getData();
                //System.out.println("=======================================================|||");
                //System.out.println("/" + this.meshName + "/services/" + service + "/" + node);
                //System.out.println(KeeperException.Code.get(event.getResultCode()));
                if (data != null) {
                    try {
                        responder.run(service, node, new ObjectMapper().readValue(data, ServiceInformation.class));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                } else {
                    responder.run(service, node, null);
                }
            }).forPath("/" + this.meshName + "/services/" + service + "/" + node);

        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void register(String path, byte[] data)  {
        try {
            zooClient.create().withMode(CreateMode.EPHEMERAL).inBackground((CuratorFramework client, CuratorEvent event)->{
                //System.out.println("=======================================================");
                //System.out.println(path);
                //System.out.println(KeeperException.Code.get(event.getResultCode()));
            }).forPath(path, data);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    Set<String> nodes = new HashSet<>();
    public class WatchNodeList implements Runnable{
        public void run(){
            while(true) {
                synchronized (nodes) {
                    for (String nodeStr : nodes) {
                        //System.out.println("Checking node "+nodeStr);
                        String[] parts = nodeStr.split(",");
                        getServiceNodeInformation(parts[1], parts[0], new DataUpdate() {
                            @Override
                            public void run(String service, String node, ServiceInformation data) {
                                if (data != null) {
                                    mesh.geteManager().sync(data);
                                } else {
                                    mesh.geteManager().delete(node);
                                    synchronized (nodes) {
                                        nodes.remove(nodeStr);
                                    }
                                }
                            }
                        }, true);
                    }
                }
                try {
                    Thread.sleep(5000);
                }catch (Exception e){

                }
            }
        }
    }
    public class SyncList implements Runnable{
        public void run(){
            while(true) {
                getServiceList(new DataUpdate() {
                    @Override
                    public void run(String path, List<String> registeredServices) {
                        if(registeredServices!=null) {
                            for (String service : registeredServices) {
                                getServiceNodeList(service, new DataUpdate() {
                                    @Override
                                    public void run(String service, List<String> serviceNodes) {
                                        for (String node : serviceNodes) {
                                            boolean exists = false;
                                            synchronized (nodes){
                                                exists = nodes.contains(node+","+service);
                                            }
                                            if(!exists) {
                                                synchronized (nodes){
                                                    exists = nodes.add(node+","+service);
                                                }
                                            }
                                        }
                                    }
                                });
                            }
                        }
                    }
                });
                try {
                    Thread.sleep(5000);
                }catch (Exception e){

                }
            }
        }
    }
}
