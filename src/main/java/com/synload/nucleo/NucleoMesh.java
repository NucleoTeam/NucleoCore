package com.synload.nucleo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.synload.nucleo.event.NucleoData;
import com.synload.nucleo.event.NucleoResponder;
import com.synload.nucleo.hub.Hub;
import com.synload.nucleo.socket.EManager;
import com.synload.nucleo.zookeeper.ZooKeeperManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.util.*;


public class NucleoMesh {
    private Hub hub;
    private String uniqueName;
    private ZooKeeperManager manager;
    private String meshName;
    private String serviceName;
    private EManager eManager;
    protected static final Logger logger = LoggerFactory.getLogger(NucleoMesh.class);

    public NucleoMesh(String meshName, String serviceName, String zookeeper, String elasticServer, int elasticPort, String packageStr) {
        this.uniqueName = UUID.randomUUID().toString();
        hub = new Hub(this, uniqueName, elasticServer, elasticPort);
        this.meshName = meshName;
        this.serviceName = serviceName;
        logger.info("Starting nucleo client and joining mesh " + meshName + " with service name " + serviceName);
        int ePort = nextAvailable();
        logger.info("Selected Port: " + ePort);
        this.eManager = new EManager(this, ePort);
        this.eManager.createServer();
        getHub().register(packageStr);
        try {
            manager = new ZooKeeperManager(zookeeper, this);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static int nextAvailable() {
        int port = (int) Math.round(Math.random() * 1000) + 8000;
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

        return nextAvailable();
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

    public ZooKeeperManager getManager() {
        return manager;
    }

    public void setManager(ZooKeeperManager manager) {
        this.manager = manager;
    }

    static long[] avg = new long[20];
    static int k = 0;
    static int counter = 0;

    public static void main(String[] args) {
        //createTopic();

        NucleoMesh mesh = new NucleoMesh("mcbans", "nucleocore", "192.168.1.7:2181", "192.168.1.7", 9200, "com.synload.nucleo.information");
        while (true) {
            mesh.call(
                new String[]{"information", "[popcorn/information.hits/information.test]", "information.test", "[popcorn/information.hits/information.test]"},
                new TreeMap<String, Object>() {{
                    put("wow", "works?");
                    put("time", System.currentTimeMillis());
                }},
                new NucleoResponder() {
                    @Override
                    public void run(NucleoData data) {
                        long totalTime = (System.currentTimeMillis() - (long) data.getObjects().get("time"));
                        if (totalTime > 50) {
                            try {
                                logger.info("timeout for: " + new ObjectMapper().writeValueAsString(data));
                            } catch (Exception e) {
                            }
                            logger.debug("total: " + totalTime + "ms");
                        } else {
                            logger.info("total: " + totalTime + "ms");
                            try {
                                logger.info("data: " + new ObjectMapper().writeValueAsString(data));
                            } catch (Exception e) {
                            }
                        }
                    }
                }
            );
            try {
                Thread.sleep(4000);
            } catch (Exception e) {
            }
        }

    }
}
