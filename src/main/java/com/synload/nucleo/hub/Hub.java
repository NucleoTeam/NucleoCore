package com.synload.nucleo.hub;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.synload.nucleo.consumer.ConsumerHandler;
import com.synload.nucleo.event.*;
import com.synload.nucleo.loader.LoadHandler;
import com.synload.nucleo.producer.ProducerHandler;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.reflections.Reflections;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.*;

public class Hub {
    private ProducerHandler producer;
    private Queue<Object[]> queue = new LinkedList<>();
    private EventHandler eventHandler = new EventHandler();
    private TreeMap<String, NucleoResponder> responders = new TreeMap();
    private TreeMap<String, Thread> timeouts = new TreeMap<>();
    private String bootstrap;
    public static ObjectMapper objectMapper = new ObjectMapper();
    private ArrayList<Integer> ready = new ArrayList<>();
    private String groupName;
    private String clientName;

    public Hub(String clientName, String bootstrap, String groupName) {
        this.bootstrap = bootstrap;
        this.groupName = groupName;
        this.clientName = clientName;
        producer = new ProducerHandler(this.bootstrap);
        int id = ready.size();
        ready.add(0);
        new Thread(
            new Listener(
                this,
                new String[]{
                    "nucleo.client." + clientName
                },
                this.bootstrap,
                id
            )
        ).start();
    }

    public void run() {
        new Thread(new Writer(this)).start();
    }

    public void push(NucleoData data, NucleoResponder responder) {
        responders.put(data.getRoot().toString(), responder);
        Thread timeout = new Thread(new NucleoTimeout(this, data));
        timeout.start();
        timeouts.put(data.getRoot().toString(), timeout);
        queue.add(new Object[]{data.getChainList().get(data.getOnChain())[data.getLink()], data});
    }

    public void register(String servicePackage) {
        try {
            Reflections reflect = new Reflections(servicePackage);
            Set<Class<?>> classes = reflect.getTypesAnnotatedWith(NucleoClass.class);
            System.out.println(new ObjectMapper().writeValueAsString(classes));
            LoadHandler.getMethods(classes.toArray()).forEach((m) -> {
                int id = ready.size();
                ready.add(0);
                new Thread(new Listener(this, getEventHandler().registerMethod(m), this.bootstrap, id)).start();
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public class Writer implements Runnable {

        private Hub hub;

        public Writer(Hub hub) {
            this.hub = hub;

        }

        public void run() {

            while (true) {
                try {
                    while (this.hub.queue.size() > 0) {
                        Object[] dataBlock = this.hub.queue.remove();
                        String topic = (String) dataBlock[0];
                        NucleoData data = (NucleoData) dataBlock[1];
                        ProducerRecord record = new ProducerRecord(
                            topic,
                            UUID.randomUUID().toString(),
                            objectMapper.writeValueAsString(data)
                        );
                        producer.getProducer().send(record);
                    }
                    Thread.sleep(1L);
                } catch (Exception e) {
                    e.printStackTrace();
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

        public Set<String> verifyPrevious(String currentChain, Set<String> checkChains){
            Set<String> previousChains = new HashSet<>();
            Set<String> checkChainsTMP = new HashSet<>(checkChains);
            for(String[] chain : data.getChainList()) {
                String prevChain = String.join(".", chain);
                if(prevChain.equals(currentChain)){
                    break;
                }else{
                    previousChains.add(prevChain);
                }
            }
            System.out.println(checkChainsTMP);
            if(previousChains.containsAll(checkChainsTMP)){
                return null;
            }else{
                data.getChainBreak().getBreakReasons();
                checkChainsTMP.removeAll(previousChains);
            }
            return checkChainsTMP;
        }
        public void run() {
            exec();
        }
        public void exec() {
            try {
                if (topic.startsWith("nucleo.client.")) {
                    System.out.println("done");
                    NucleoResponder responder = responders.get(data.getRoot().toString());
                    if (responder != null) {
                        responders.remove(data.getRoot().toString());
                        Thread timeout = timeouts.get(data.getRoot().toString());
                        if (timeout != null) {
                            timeout.interrupt();
                            timeouts.remove(data.getRoot().toString());
                        }
                        data.getExecution().setEnd(System.currentTimeMillis());
                        responder.run(data);
                    }
                } else if (eventHandler.getChainToMethod().containsKey(topic)) {
                    Object[] methodData = eventHandler.getChainToMethod().get(topic);
                    NucleoStep timing = new NucleoStep(topic, System.currentTimeMillis());
                    if(methodData[2]!=null) {
                        Set<String> missingChains;
                        if((missingChains = verifyPrevious(topic, (Set<String>)methodData[2]))!=null){
                            timing.setEnd(System.currentTimeMillis());
                            data.getChainBreak().setBreakChain(true);
                            data.getChainBreak().getBreakReasons().add("Missing required chains "+missingChains+"!");
                            data.getSteps().add(timing);
                            queue.add(new Object[]{"nucleo.client." + data.getOrigin(), data});
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
                                queue.add(new Object[]{"nucleo.client." + data.getOrigin(), data});
                                return;
                            }
                            boolean sameChain = false;
                            if (data.getLink() + 1 == data.getChainList().get(data.getOnChain()).length) {
                                if (data.getChainList().size() == data.getOnChain() + 1) {
                                    timing.setEnd(System.currentTimeMillis());
                                    data.getSteps().add(timing);
                                    queue.add(new Object[]{"nucleo.client." + data.getOrigin(), data});
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
                            String newTopic = getTopic(data);
                            if(sameChain){
                                if(eventHandler.getChainToMethod().containsKey(newTopic)){
                                    topic=newTopic;
                                    exec();
                                    return;
                                }
                            }
                            queue.add(new Object[]{ newTopic, data});
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
                        responder.run(data);
                    }
                } else {
                    System.out.println("Topic or responder not found: " + topic);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public class Listener implements Runnable {
        private ConsumerHandler consumer;
        private String[] topics;
        private Hub hub;
        private int id;

        public Listener(Hub hub, String[] topics, String bootstrap, int id) {
            this.hub = hub;
            this.id = id;
            this.topics = topics;
            consumer = new ConsumerHandler(bootstrap, groupName);
            consumer.getConsumer().unsubscribe();
            consumer.subscribe(topics);
        }

        public void run() {
            consumer.getConsumer().commitAsync();
            ObjectMapper objectMapper = new ObjectMapper();
            ready.set(id, 1);
            while (true) {
                try {
                    ConsumerRecords<Integer, String> consumerRecords = consumer.getConsumer().poll(Duration.ofMillis(100));
                    if (consumerRecords != null) {
                        consumerRecords.forEach(record -> {
                            try {
                                NucleoData data = objectMapper.readValue(record.value(), NucleoData.class);
                                new Thread(new Executor(hub, data, record.topic())).start();
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        });
                        consumer.getConsumer().commitAsync();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public ProducerHandler getProducer() {
        return producer;
    }

    public void setProducer(ProducerHandler producer) {
        this.producer = producer;
    }

    public Queue<Object[]> getQueue() {
        return queue;
    }

    public void setQueue(Queue<Object[]> queue) {
        this.queue = queue;
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

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public String getClientName() {
        return clientName;
    }

    public void setClientName(String clientName) {
        this.clientName = clientName;
    }

    public TreeMap<String, Thread> getTimeouts() {
        return timeouts;
    }

    public void setTimeouts(TreeMap<String, Thread> timeouts) {
        this.timeouts = timeouts;
    }
}
