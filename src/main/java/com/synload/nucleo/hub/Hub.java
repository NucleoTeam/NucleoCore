package com.synload.nucleo.hub;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.synload.nucleo.NucleoMesh;
import com.synload.nucleo.elastic.ElasticSearchPusher;
import com.synload.nucleo.event.*;
import com.synload.nucleo.loader.LoadHandler;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.reflections.Reflections;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class Hub {
    private EventHandler eventHandler = new EventHandler();
    private TreeMap<String, NucleoResponder> responders = new TreeMap();
    private TreeMap<String, Thread> timeouts = new TreeMap<>();
    private String bootstrap;
    public static ObjectMapper objectMapper = new ObjectMapper();
    private ArrayList<Integer> ready = new ArrayList<>();
    private String uniqueName;
    private ElasticSearchPusher esPusher;
    private NucleoMesh mesh;
    private Writer writer;


    public Hub(NucleoMesh mesh, String uniqueName, String elasticServer, int elasticPort) {
        this.uniqueName = uniqueName;
        this.mesh = mesh;
        esPusher = new ElasticSearchPusher(elasticServer, elasticPort, "http");
        int id = ready.size();
        ready.add(0);
        new Thread(
          esPusher
        ).start();
    }

    public NucleoData constructNucleoData(String chain, TreeMap<String, Object> objects){
        NucleoData data = new NucleoData();
        data.setObjects(objects);
        data.setOrigin(uniqueName);
        data.setLink(0);
        data.setOnChain(0);
        data.getChainList().add(chain.split("\\."));
        return data;
    }

    public NucleoData constructNucleoData(String[] chains, TreeMap<String, Object> objects){
        NucleoData data = new NucleoData();
        data.setObjects(objects);
        data.setTimeTrack(System.currentTimeMillis());
        data.setOrigin(uniqueName);
        data.setLink(0);
        data.setOnChain(0);
        for(String chain : chains) {
            data.getChainList().add(chain.split("\\."));
        }
        return data;
    }

    public void run() {
        writer = new Writer(this);
        new Thread(writer).start();
    }

    public void push(NucleoData data, NucleoResponder responder, boolean allowTracking) {
        responders.put(data.getRoot().toString(), responder);
        if (allowTracking) {
            /*Thread timeout = new Thread(new NucleoTimeout(this, data));
            timeout.start();
            synchronized (timeouts){
                timeouts.put(data.getRoot().toString(), timeout);
            }*/
        }else{
            data.setTrack(0);
        }
        writer.add(new Object[]{data.getChainList().get(data.getOnChain())[data.getLink()], data});
    }

    public void register(String servicePackage) {
        try {
            Reflections reflect = new Reflections(servicePackage);
            Set<Class<?>> classes = reflect.getTypesAnnotatedWith(NucleoClass.class);
            LoadHandler.getMethods(classes.toArray()).forEach((m) -> {
                int id = ready.size();
                ready.add(0);
                getEventHandler().registerMethod(m);
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public class Writer implements Runnable {

        private Hub hub;
        private Stack<Object[]> queue = new Stack<>();
        public CountDownLatch latch = new CountDownLatch(1);

        public Writer(Hub hub) {
            this.hub = hub;
        }

        public synchronized void add(Object[] item){
            queue.add(item);
            latch.countDown();
        }

        public void run() {

            while (true) {
                try {
                    latch.await();
                    while (!queue.isEmpty()) {
                        Object[] dataBlock = queue.pop();
                        String topic = (String) dataBlock[0];
                        NucleoData data = (NucleoData) dataBlock[1];
                        this.hub.mesh.geteManager().robin(topic, data);
                    }
                    latch = new CountDownLatch(1);
                } catch (Exception e) {
                    //e.printStackTrace();
                }
            }
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
                chains += "." + data.getChainList().get(data.getOnChain())[i];
            }
            return chains;
        }

        public Set<String> verifyPrevious(Set<String> checkChains){
            Set<String> previousChains = new HashSet<>();
            Set<String> checkChainsTMP = new HashSet<>(checkChains);
            data.getSteps().stream().filter(s->s.getEnd()>0).forEach(s->previousChains.add(s.getStep()));
            System.out.println("------");
            System.out.println(previousChains);
            System.out.println(checkChainsTMP);
            if(previousChains.containsAll(checkChainsTMP)){
                return null;
            }
            data.getChainBreak().getBreakReasons();
            checkChainsTMP.removeAll(previousChains);
            return checkChainsTMP;
        }
        public void run() {
            try {
                if (topic.startsWith("nucleo.client.")) {
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
                    if(methodData[2]!=null) {
                        Set<String> missingChains;
                        if((missingChains = verifyPrevious((Set<String>)methodData[2]))!=null){
                            timing.setEnd(System.currentTimeMillis());
                            data.getChainBreak().setBreakChain(true);
                            data.getChainBreak().getBreakReasons().add("Missing required chains "+missingChains+"!");
                            data.getSteps().add(timing);
                            esPusher.add(data);
                            writer.add(new Object[]{"nucleo.client." + data.getOrigin(), data});
                            return;
                        }
                    }
                    Object obj;
                    if (methodData[0] instanceof Class) {
                        Class clazz = (Class) methodData[0];
                        obj = clazz.getDeclaredConstructor().newInstance();
                    } else {
                        obj = methodData[0];
                    }
                    Method method = (Method) methodData[1];
                    NucleoResponder responder = new NucleoResponder(){
                        public void run(NucleoData data){
                        if (data.getChainBreak().isBreakChain()) {
                            timing.setEnd(System.currentTimeMillis());
                            data.getSteps().add(timing);
                            esPusher.add(data);
                            writer.add(new Object[]{"nucleo.client." + data.getOrigin(), data});
                            return;
                        }
                        boolean sameChain = false;
                        if (data.getLink() + 1 == data.getChainList().get(data.getOnChain()).length) {
                            if (data.getChainList().size() == data.getOnChain() + 1) {
                                timing.setEnd(System.currentTimeMillis());
                                data.getSteps().add(timing);
                                esPusher.add(data);
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
                        if(sameChain){
                            if(eventHandler.getChainToMethod().containsKey(newTopic)){
                                topic=newTopic;
                                //Executor.this.run();
                                //return;
                            }
                        }
                        writer.add(new Object[]{ newTopic, data});
                        }
                    };
                    int len = method.getParameterTypes().length;
                    if(len>0){
                        if(method.getParameterTypes()[0]==NucleoData.class && len==1){
                            try{
                                method.invoke(obj, data);
                            }catch (Exception e){
                                e.printStackTrace();
                            }
                            responder.run(data);
                        }else if(method.getParameterTypes()[0]==NucleoData.class && len==2 && method.getParameterTypes()[1]==NucleoResponder.class){
                            try{
                                method.invoke(obj, data, responder);
                            }catch (Exception e){
                                e.printStackTrace();
                            }
                        }
                    }else{
                        try{
                            method.invoke(obj);
                        }catch (Exception e){
                            e.printStackTrace();
                        }
                        responder.run();
                    }
                } else {
                    System.out.println("Topic or responder not found: " + topic);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void handle(Hub hub, NucleoData data, String topic){
        new Thread(new Executor(hub, data, topic)).start();
    }

    public EventHandler getEventHandler() {
        return eventHandler;
    }

    public void setEventHandler(EventHandler eventHandler) {
        this.eventHandler = eventHandler;
    }

    public TreeMap<String, NucleoResponder> getResponders() {
        return responders;
    }

    public void setResponders(TreeMap<String, NucleoResponder> responders) {
        this.responders = responders;
    }

    public boolean isReady() {
        return ready.contains(0);
    }

    public String getBootstrap() {
        return bootstrap;
    }

    public void setBootstrap(String bootstrap) {
        this.bootstrap = bootstrap;
    }

    public ArrayList<Integer> getReady() {
        return ready;
    }

    public void setReady(ArrayList<Integer> ready) {
        this.ready = ready;
    }

    public TreeMap<String, Thread> getTimeouts() {
        return timeouts;
    }

    public void setTimeouts(TreeMap<String, Thread> timeouts) {
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
