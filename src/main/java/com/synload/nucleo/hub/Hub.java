package com.synload.nucleo.hub;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.synload.nucleo.consumer.ConsumerHandler;
import com.synload.nucleo.event.EventHandler;
import com.synload.nucleo.event.NucleoData;
import com.synload.nucleo.event.NucleoResponder;
import com.synload.nucleo.loader.LoadHandler;
import com.synload.nucleo.producer.ProducerHandler;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.Future;

public class Hub {
  private ProducerHandler producer;
  private Queue<Object[]> queue = new LinkedList<>();
  private EventHandler eventHandler = new EventHandler();
  private TreeMap<String, NucleoResponder> responders = new TreeMap();
  private String bootstrap;
  private ArrayList<Integer> ready = new ArrayList<>();
  private String groupName;
  private String clientName;

  public Hub(String bootstrap, String clientName, String groupName) {
    this.bootstrap = bootstrap;
    this.groupName = groupName;
    this.clientName = clientName;
    producer = new ProducerHandler(this.bootstrap);
    int id = ready.size();
    ready.add(0);
    new Thread(new Listener(this, new String[]{"nucleo.client."+clientName}, this.bootstrap, id)).start();
  }
  public void run(){
    new Thread(new Writer(this)).start();
  }

  public void push(NucleoData data, NucleoResponder responder){
    responders.put(data.getRoot().toString(), responder);
    queue.add(new Object[]{data.getChain()[data.getLink()], data});
  }
  public <T> void register(T... clazzez){
    try {
      LoadHandler.getMethods(clazzez).forEach((m) -> {
        int id = ready.size();
        ready.add(0);
        new Thread(new Listener(this, getEventHandler().registerMethod(m), this.bootstrap, id)).start();
      });
    }catch (Exception e){
      e.printStackTrace();
    }
  }
  public class Writer implements Runnable {

    private Hub hub;

    public Writer(Hub hub){
      this.hub = hub;

    }

    public void run(){
      ObjectMapper objectMapper = new ObjectMapper();
      while (true) {
        try {
          while(this.hub.queue.size()>0) {

            Object[] dataBlock = this.hub.queue.remove();
            String topic = (String)dataBlock[0];
            NucleoData data = (NucleoData)dataBlock[1];

            if(data.getChain().length-1!=data.getLink() && data.getOrigin().equals(clientName)) {
                NucleoResponder responder = new NucleoResponder() {
                  public void run(NucleoData data) {
                    String chain;
                    if(!data.getChainBreak().isBreakChain()) {
                      data.setLink(data.getLink() + 1);
                      chain = data.getChain()[0];
                      for (int i = 1; i <= data.getLink(); i++) {
                        chain += "." + data.getChain()[i];
                      }
                    } else {
                      chain = "nucleo.client."+data.getOrigin();
                    }
                    queue.add(new Object[]{chain, data});
                  }
                };
                UUID responderUUID = UUID.randomUUID();
                data.setUuid(responderUUID);
                responders.put(data.getUuid().toString(), responder);
            }
            ProducerRecord record = new ProducerRecord(
              topic,
              UUID.randomUUID().toString(),
              objectMapper.writeValueAsString(data)
            );

            String topicsAll = "";
            Future x = producer.getProducer().send(record);
            RecordMetadata metadata = (RecordMetadata) x.get();
            if(data.getOrigin().equals(clientName)) {
              new Thread(new Runnable() {
                @Override
                public void run() {
                  try {
                    Thread.sleep(5000);
                    if(responders.containsKey(data.getUuid().toString())) {
                      System.out.println(new ObjectMapper().writeValueAsString(data));
                      responders.get(data.getUuid().toString()).run(data);
                    }
                  } catch (Exception e) {
                    e.printStackTrace();
                  }

                }
              }).start();
            }
            //System.out.println(metadata.topic()+" Partition: " + metadata.partition());
            //System.out.println(metadata.topic()+" Size:" + metadata.serializedValueSize());
            //System.out.println(metadata.topic()+" Timestamp: " + metadata.timestamp());
          }
          Thread.sleep(1L);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
  }
  public class Listener implements Runnable {
    private ConsumerHandler consumer;
    private String[] topics;
    private Hub hub;
    private int id;

    public Listener(Hub hub, String[] topics, String bootstrap, int id){
      this.hub = hub;
      this.id = id;
      this.topics = topics;
      consumer = new ConsumerHandler(bootstrap, groupName);
      consumer.getConsumer().unsubscribe();
      consumer.subscribe(topics);
    }

    public void run(){
      consumer.getConsumer().commitAsync();
      ObjectMapper objectMapper = new ObjectMapper();
      ready.set(id, 1);
      while (true) {
        ConsumerRecords<Integer, String> consumerRecords = consumer.getConsumer().poll(Duration.ofMillis(1));
        if(consumerRecords!=null) {
          consumerRecords.forEach(record -> {
            try {
              NucleoData data = objectMapper.readValue(record.value(), NucleoData.class);
              if(data.getChainBreak().isBreakChain() && data.getOrigin().equals(clientName)) {
                responders.get(data.getRoot().toString()).run(data);
                responders.remove(data.getRoot().toString());
              }else if (eventHandler.getChainToMethod().containsKey(record.topic())) {
                Object[] methodData = eventHandler.getChainToMethod().get(record.topic());
                Object obj;
                if(methodData[0] instanceof Class) {
                  Class clazz = (Class) methodData[0];
                  obj = clazz.newInstance();
                }else{
                  obj = methodData[0];
                }
                Method method = (Method) methodData[1];
                method.invoke(obj, data);
                queue.add(new Object[]{ "nucleo.client."+data.getOrigin(), data });
              }else if(data.getUuid()!=null && responders.containsKey(data.getUuid().toString())){
                responders.get(data.getUuid().toString()).run(data);
                responders.remove(data.getUuid().toString());
              }else if(responders.containsKey(data.getRoot().toString())){
                responders.get(data.getRoot().toString()).run(data);
                responders.remove(data.getRoot().toString());
              } else {
                System.out.println("Topic or responder not found: "  + record.topic());
              }
            }catch (Exception e){
              e.printStackTrace();
            }

            String topicsAll = "";
            try {
              topicsAll=new ObjectMapper().writeValueAsString(topics);
            }catch (Exception e){
              e.printStackTrace();
            }
            //System.out.println(topicsAll+" Record Key " + record.key());
            //System.out.println(topicsAll+" Record value " + record.value());
            //System.out.println(topicsAll+" Record partition " + record.partition());
            //System.out.println(topicsAll+" Record offset " + record.offset());
          });
          consumer.getConsumer().commitAsync();
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

  public boolean isReady(){
    return ready.contains(0);
  }
}
