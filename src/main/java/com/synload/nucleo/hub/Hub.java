package com.synload.nucleo.hub;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.collect.Tables;
import com.synload.nucleo.NucleoMesh;
import com.synload.nucleo.elastic.ElasticSearchPusher;
import com.synload.nucleo.event.*;
import com.synload.nucleo.loader.LoadHandler;
import org.reflections.Reflections;

import java.lang.reflect.Method;
import java.util.*;

public class Hub {
    private EventHandler eventHandler = new EventHandler();
    private HashMap<String, NucleoResponder> responders = Maps.newHashMap();
    private HashMap<String, Thread> timeouts = Maps.newHashMap();
    private String bootstrap;
    public static ObjectMapper objectMapper = new ObjectMapper();
    private String uniqueName;
    private ElasticSearchPusher esPusher;
    private NucleoMesh mesh;
    private Writer writer;


    public Hub(NucleoMesh mesh, String uniqueName, String elasticServer, int elasticPort) {
        this.uniqueName = uniqueName;
        this.mesh = mesh;
        esPusher = new ElasticSearchPusher(elasticServer, elasticPort, "http");
        new Thread(
            esPusher
        ).start();
    }

    public NucleoData constructNucleoData(String chain, TreeMap<String, Object> objects) {
        NucleoData data = new NucleoData();
        data.setObjects(objects);
        data.setOrigin(uniqueName);
        data.setLink(0);
        data.setOnChain(0);
        data.getChainList().add(chain.split("\\."));
        return data;
    }

    public NucleoData constructNucleoData(String[] chains, TreeMap<String, Object> objects) {
        NucleoData data = new NucleoData();
        data.setObjects(objects);
        data.setTimeTrack(System.currentTimeMillis());
        data.setOrigin(uniqueName);
        data.setLink(0);
        data.setOnChain(0);
        for (String chain : chains) {
            data.getChainList().add(chain.split("\\."));
        }
        return data;
    }

    public void run() {
        writer = new Writer(this);
    }

    public void push(NucleoData data, NucleoResponder responder, boolean allowTracking) {
        responders.put(data.getRoot().toString(), responder);
        if (allowTracking) {
            Thread timeout = new Thread(new NucleoTimeout(this, data));
            timeout.start();
            synchronized (timeouts) {
                timeouts.put(data.getRoot().toString(), timeout);
            }
        } else {
            data.setTrack(0);
        }
        writer.add(new Object[]{data.getChainList().get(data.getOnChain())[data.getLink()], data});
    }

    public void register(String servicePackage) {
        try {
            Reflections reflect = new Reflections(servicePackage);
            Set<Class<?>> classes = reflect.getTypesAnnotatedWith(NucleoClass.class);
            LoadHandler.getMethods(classes.toArray()).forEach((m) -> getEventHandler().registerMethod(m));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public class Writer {

        private Hub hub;

        public Writer(Hub hub) {
            this.hub = hub;
        }

        public synchronized void add(Object[] item) {
            String topic = (String) item[0];
            NucleoData data = (NucleoData) item[1];
            data.markTime("Queue Done, sending to round robin");
            this.hub.mesh.geteManager().robin(topic, data);
        }
    }

    public class Executor implements Runnable {
        public Hub hub;
        public NucleoData data;
        public String topic;


        public Executor(Hub hub, NucleoData data, String topic) {
            this.hub = hub;
            this.data = data;
            this.topic = topic;
        }

        public String getTopic(NucleoData data) {
            String chains = data.getChainList().get(data.getOnChain())[0];
            for (int i = 1; i <= data.getLink(); i++) {
                if(data.getChainList().get(data.getOnChain()).length>i) {
                    chains += "." + data.getChainList().get(data.getOnChain())[i];
                }
            }
            return chains;
        }

        public List<String> verifyPrevious(List<String> checkChains) {
            List<String> previousChains = Lists.newArrayList();
            List<String> checkChainsTMP = Lists.newArrayList(checkChains);
            data.getSteps().stream().filter(s -> s.getEnd() > 0).forEach(s -> previousChains.add(s.getStep()));
            System.out.println("------");
            System.out.println(previousChains);
            System.out.println(checkChainsTMP);
            if (previousChains.containsAll(checkChainsTMP)) {
                return null;
            }
            data.getChainBreak().getBreakReasons();
            checkChainsTMP.removeAll(previousChains);
            return checkChainsTMP;
        }

        public void run() {
            try {
                data.markTime("Start Execution on " + uniqueName);
                if (topic.startsWith("nucleo.client.")) {
                    if (data.getObjects().containsKey("_ping")) {
                        Stack<String> hosts = (Stack<String>) data.getObjects().get("ping");
                        if (hosts != null && !hosts.isEmpty()) {
                            String host = hosts.pop();
                            //System.out.println("going to: nucleo.client." + host);
                            data.markTime("Execution Complete");
                            writer.add(new Object[]{"nucleo.client." + host, data});
                            return;
                        } else {
                            esPusher.add(data);
                            data.getObjects().remove("_ping");
                            data.markTime("Execution Complete");
                            writer.add(new Object[]{"nucleo.client." + data.getOrigin(), data});
                            //System.out.println("ping going home!");
                            return;
                        }
                    }
                    if (data.getObjects().containsKey("_route")) {
                        List<String> hosts = (List<String>) data.getObjects().get("_route");
                        if (hosts != null && !hosts.isEmpty()) { // route not complete
                            String host = hosts.remove(0);
                            if(host.equals(uniqueName)) {
                                data.getObjects().remove("_route");
                                data.markTime("Route Complete");
                                topic = getTopic(data);
                                run();
                                return;
                            }
                            //System.out.println("going to: nucleo.client." + host);
                            data.markTime("Sending to "+host);
                            writer.add(new Object[]{"nucleo.client." + host, data});
                            return;
                        }else{
                            data.getObjects().remove("_route");
                            data.markTime("Route Complete");
                            topic = getTopic(data);
                            run();
                            return;
                        }
                    }
                    NucleoResponder responder = responders.get(data.getRoot().toString());
                    if (responder != null) {
                        responders.remove(data.getRoot().toString());
                        Thread timeout = timeouts.get(data.getRoot().toString());
                        if (timeout != null) {
                            timeout.interrupt();
                            timeouts.remove(data.getRoot().toString());
                        }
                        data.getExecution().setEnd(System.currentTimeMillis());
                        esPusher.add(data);
                        data.markTime("Execution Complete");
                        responder.run(data);
                        //System.out.println("response: " + data.markTime() + "ms");
                        if (data.getTrack() == 1) {
                            hub.push(hub.constructNucleoData(new String[]{"_watch.complete"}, new TreeMap<String, Object>() {{
                                put("root", data.getRoot());
                            }}), new NucleoResponder() {
                                @Override
                                public void run(NucleoData returnedData) {
                                }
                            }, false);
                        }
                        return;
                    }
                } else if (eventHandler.getChainToMethod().containsKey(topic)) {
                    Object[] methodData = eventHandler.getChainToMethod().get(topic);
                    NucleoStep timing = new NucleoStep(topic, System.currentTimeMillis());
                    if (methodData[2] != null) {
                        List<String> missingChains;
                        if ((missingChains = verifyPrevious((List<String>) methodData[2])) != null) {
                            timing.setEnd(System.currentTimeMillis());
                            data.getChainBreak().setBreakChain(true);
                            data.getChainBreak().getBreakReasons().add("Missing required chains " + missingChains + "!");
                            data.getSteps().add(timing);
                            esPusher.add(data);
                            data.markTime("Execution Complete");
                            writer.add(new Object[]{"nucleo.client." + data.getOrigin(), data});
                            return;
                        }
                    }
                    data.markTime("Verified Chain Requirements");
                    Object obj;
                    if (methodData[0] instanceof Class) {
                        Class clazz = (Class) methodData[0];
                        obj = clazz.getDeclaredConstructor().newInstance();
                    } else {
                        obj = methodData[0];
                    }
                    Method method = (Method) methodData[1];
                    NucleoResponder responder = new NucleoResponder() {
                        public void run(NucleoData data) {
                            if (data.getChainBreak().isBreakChain()) {
                                timing.setEnd(System.currentTimeMillis());
                                data.getSteps().add(timing);
                                esPusher.add(data);
                                data.markTime("Execution Complete");
                                writer.add(new Object[]{"nucleo.client." + data.getOrigin(), data});
                                return;
                            }
                            boolean sameChain = false;
                            if (data.getLink() + 1 == data.getChainList().get(data.getOnChain()).length) {
                                if (data.getChainList().size() == data.getOnChain() + 1) {
                                    timing.setEnd(System.currentTimeMillis());
                                    data.getSteps().add(timing);
                                    esPusher.add(data);
                                    data.markTime("Execution Complete");
                                    writer.add(new Object[]{"nucleo.client." + data.getOrigin(), data});
                                    return;
                                } else {
                                    data.setOnChain(data.getOnChain() + 1);
                                    data.setLink(0);
                                }
                            } else {
                                data.setLink(data.getLink() + 1);
                                sameChain = true;
                            }
                            timing.setEnd(System.currentTimeMillis());
                            data.getSteps().add(timing);
                            esPusher.add(data);
                            String newTopic = getTopic(data);
                            if (sameChain) {
                                if (eventHandler.getChainToMethod().containsKey(newTopic)) {
                                    topic = newTopic;
                                    Executor.this.run();
                                    return;
                                }
                            }
                            data.markTime("Execution Complete");
                            writer.add(new Object[]{newTopic, data});
                        }
                    };
                    int len = method.getParameterTypes().length;
                    if (len > 0) {
                        if (method.getParameterTypes()[0] == NucleoData.class && len == 1) {
                            try {
                                method.invoke(obj, data);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                            responder.run(data);
                        } else if (method.getParameterTypes()[0] == NucleoData.class && len == 2 && method.getParameterTypes()[1] == NucleoResponder.class) {
                            try {
                                method.invoke(obj, data, responder);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    } else {
                        try {
                            method.invoke(obj);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        responder.run();
                    }
                } else {
                    //System.out.println("Topic or responder not found: " + topic);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void handle(Hub hub, NucleoData data, String topic) {
        new Thread(new Executor(hub, data, topic)).start();
    }

    public EventHandler getEventHandler() {
        return eventHandler;
    }

    public void setEventHandler(EventHandler eventHandler) {
        this.eventHandler = eventHandler;
    }

    public HashMap<String, NucleoResponder> getResponders() {
        return responders;
    }

    public void setResponders(HashMap<String, NucleoResponder> responders) {
        this.responders = responders;
    }


    public String getBootstrap() {
        return bootstrap;
    }

    public void setBootstrap(String bootstrap) {
        this.bootstrap = bootstrap;
    }

    public HashMap<String, Thread> getTimeouts() {
        return timeouts;
    }

    public void setTimeouts(HashMap<String, Thread> timeouts) {
        this.timeouts = timeouts;
    }

    public static ObjectMapper getObjectMapper() {
        return objectMapper;
    }

    public static void setObjectMapper(ObjectMapper objectMapper) {
        Hub.objectMapper = objectMapper;
    }

    public String getUniqueName() {
        return uniqueName;
    }

    public void setUniqueName(String uniqueName) {
        this.uniqueName = uniqueName;
    }

    public ElasticSearchPusher getEsPusher() {
        return esPusher;
    }

    public void setEsPusher(ElasticSearchPusher esPusher) {
        this.esPusher = esPusher;
    }

    public NucleoMesh getMesh() {
        return mesh;
    }

    public void setMesh(NucleoMesh mesh) {
        this.mesh = mesh;
    }

    public Writer getWriter() {
        return writer;
    }

    public void setWriter(Writer writer) {
        this.writer = writer;
    }
}
