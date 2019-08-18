package com.synload.nucleo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.synload.nucleo.event.NucleoData;
import com.synload.nucleo.event.NucleoResponder;
import com.synload.nucleo.hub.Hub;
import com.synload.nucleo.information.HitsHandler;
import com.synload.nucleo.information.InformationHandler;
import com.synload.nucleo.loader.LoadHandler;
import com.synload.nucleo.socket.EManager;
import com.synload.nucleo.zookeeper.DataUpdate;
import com.synload.nucleo.zookeeper.Manager;
import com.synload.nucleo.zookeeper.ManagerImpl;
import com.synload.nucleo.zookeeper.ServiceInformation;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.*;


public class NucleoMesh {
    private Hub hub;
    private String uniqueName;
    private Manager manager;
    private String meshName;
    private String serviceName;
    private EManager eManager;

    public NucleoMesh(String meshName, String serviceName, String zookeeper, String elasticServer, int elasticPort) {
        this.uniqueName = UUID.randomUUID().toString();
        hub = new Hub(this, uniqueName, elasticServer, elasticPort);
        this.meshName = meshName;
        this.serviceName = serviceName;
        int ePort = nextAvailable(8000);
        this.eManager = new EManager(this, ePort);
        this.eManager.createServer();
        try {
            manager = new ManagerImpl(zookeeper, this);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void zookeeperConnected() throws IOException, KeeperException, InterruptedException {
        System.out.println("UpSet service "+serviceName+" to zookeeper");
        manager.createBlock("/" + meshName + "/services/" + serviceName);
        System.out.println("Registering this service to zookeeper");
        manager.create(
            "/" + meshName + "/services/" + serviceName + "/" + uniqueName,
            new ObjectMapper().writeValueAsBytes(new ServiceInformation(
                meshName,
                serviceName,
                this.uniqueName,
                getHub().getEventHandler().getChainToMethod().keySet(),
                InetAddress.getLocalHost().getHostAddress() + ":" + eManager.getPort(),
                InetAddress.getLocalHost().getHostName()
            ))
        );
        System.out.println("Starting zookeeper sync");
        new Thread(new SyncList()).start();
    }

    public class SyncList implements Runnable{
        public void run(){
            while(true) {
                manager.getServiceList(new DataUpdate() {
                    @Override
                    public void run(String path, List<String> registeredServices) {
                        if(registeredServices!=null) {
                            for (String service : registeredServices) {
                                manager.getServiceNodeList(service, new DataUpdate() {
                                    @Override
                                    public void run(String service, List<String> serviceNodes) {
                                        for (String node : serviceNodes) {
                                            manager.getServiceNodeInformation(service, node, new DataUpdate() {
                                                @Override
                                                public void run(String service, String node, ServiceInformation data) {
                                                    if (data != null) {
                                                        eManager.sync(data);
                                                    } else {
                                                        eManager.delete(node);
                                                    }
                                                }
                                            }, true);
                                        }
                                    }
                                });
                            }
                        }
                    }
                });
                try {
                    Thread.sleep(1000);
                }catch (Exception e){

                }
            }
        }
    }

    public void start() {
        try {
            getHub().run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void register(String packageStr) {
        getHub().register(packageStr);
    }

    public static int nextAvailable(int port) {
        if (port < 8000 || port > 9000) {
            throw new IllegalArgumentException("Invalid start port: " + port);
        }
        ServerSocket ss = null;
        DatagramSocket ds = null;
        try {
            ss = new ServerSocket(port);
            ss.setReuseAddress(true);
            ds = new DatagramSocket(port);
            ds.setReuseAddress(true);
            return port;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (ds != null) {
                ds.close();
            }

            if (ss != null) {
                try {
                    ss.close();
                } catch (IOException e) {
                    /* should not be thrown */
                }
            }
        }

        return nextAvailable(port + 1);
    }

    public void call(String chain, TreeMap<String, Object> objects, Method onFinishedMethod, Object onFinishedObject) {
        this.getHub().push(hub.constructNucleoData(chain, objects), new NucleoResponder() {
            @Override
            public void run(NucleoData returnedData) {
                try {
                    onFinishedMethod.invoke(onFinishedObject, returnedData);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }, true);
    }

    public void call(String chain, TreeMap<String, Object> objects, NucleoResponder nucleoResponder) {
        this.getHub().push(hub.constructNucleoData(chain, objects), nucleoResponder, true);
    }

    public boolean call(String[] chains, TreeMap<String, Object> objects, NucleoResponder nucleoResponder) {
        if (chains.length == 0) {
            return false;
        }
        this.getHub().push(hub.constructNucleoData(chains, objects), nucleoResponder, true);
        return true;
    }

    public Hub getHub() {
        return hub;
    }

    public void setHub(Hub hub) {
        this.hub = hub;
    }

    public String getUniqueName() {
        return uniqueName;
    }

    public void setUniqueName(String uniqueName) {
        this.uniqueName = uniqueName;
    }

    public Manager getManager() {
        return manager;
    }

    public void setManager(Manager manager) {
        this.manager = manager;
    }

    public String getMeshName() {
        return meshName;
    }

    public void setMeshName(String meshName) {
        this.meshName = meshName;
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public EManager geteManager() {
        return eManager;
    }

    public void seteManager(EManager eManager) {
        this.eManager = eManager;
    }
    static long[] avg = new long[20];
    static int k = 0;
    static int counter = 0;
    public static void main(String[] args) {
        //createTopic();
        Logger.getRootLogger().setLevel(Level.DEBUG);
        NucleoMesh mesh = new NucleoMesh("test", "nucleocore", "192.168.1.29:2181", "192.168.1.29", 9200);
        mesh.register("com.synload.nucleo.information");
        mesh.start();

        while (true) {
            mesh.call(
                new String[]{"information.hits", "information"},
                new TreeMap<String, Object>() {{
                    put("wow", "works?");
                }},
                new NucleoResponder() {
                    @Override
                    public void run(NucleoData data) {
                        //System.out.println("total: "+data.markTime()+"ms");
                    }
                }
            );
            try {
                Thread.sleep(1);
            } catch (Exception e) {

            }
        }
    }
}
