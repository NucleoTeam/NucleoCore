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

  public Hub(String bootstrap, String clientName) {
    this.bootstrap = bootstrap;
    producer = new ProducerHandler(this.bootstrap);
    new Thread(new Listener(this, "nucleo.client."+clientName, this.bootstrap)).start();
  }
  public void run(){

    new Thread(new Writer(this)).start();
  }

  public void push(NucleoData data, NucleoResponder responder){
    responders.put(data.getRoot().toString(), responder);
    queue.add(new Object[]{data.getChain()[data.getLink()], data});
  }
  public void register(Class... clazzez){
    LoadHandler.getMethods(clazzez).forEach((m)->new Thread(new Listener(this, getEventHandler().registerMethod(m), this.bootstrap)).start());
  }
  public void register(Object... clazzez){
    LoadHandler.getMethods(clazzez).forEach((m)->new Thread(new Listener(this, getEventHandler().registerMethod(m), this.bootstrap)).start());
  }
  public class Writer implements Runnable {

    private Hub hub;
    public Writer(Hub hub){
      this.hub = hub;

    }

    public void run(){
      ObjectMapper objectMapper = new ObjectMapper();
      int key = 0;
      while (true) {
        try {
          while(this.hub.queue.size()>0) {
            Object[] dataBlock = this.hub.queue.remove();
            String topic = (String)dataBlock[0];
            NucleoData data = (NucleoData)dataBlock[1];
            if(data.getChain().length-1!=data.getLink()) {
              NucleoResponder responder = new NucleoResponder() {
                public void run(NucleoData data) {
                  data.setLink(data.getLink()+1);
                  String chain = data.getChain()[0];
                  for(int i=1;i<=data.getLink();i++){
                    chain+="."+data.getChain()[i];
                  }
                  queue.add(new Object[]{chain, data});
                }
              };
              UUID responderUUID = UUID.randomUUID();
              data.setUuid(responderUUID);
              responders.put(responderUUID.toString(), responder);
            }else{
              data.setUuid(data.getRoot());
            }
            ProducerRecord record = new ProducerRecord(
              topic,
              UUID.randomUUID().toString(),
              objectMapper.writeValueAsString(data)
            );
            Future x = producer.getProducer().send(record);
            RecordMetadata metadata = (RecordMetadata) x.get();
            System.out.println(metadata.topic());
            System.out.println(metadata.partition());
            System.out.println(metadata.serializedValueSize());
            System.out.println(metadata.timestamp());
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
    private String topic;
    private Hub hub;

    public Listener(Hub hub, String topic, String bootstrap){
      this.hub = hub;
      this.topic = topic;
      consumer = new ConsumerHandler(bootstrap);
      consumer.getConsumer().unsubscribe();
      consumer.subscribe(topic);
    }

    public void run(){
      consumer.getConsumer().commitAsync();
      ObjectMapper objectMapper = new ObjectMapper();
      while (true) {
        ConsumerRecords<Integer, String> consumerRecords = consumer.getConsumer().poll(Duration.ofMillis(1));
        if(consumerRecords!=null) {
          consumerRecords.forEach(record -> {
            System.out.println("Record found!");
            try {
              NucleoData data = objectMapper.readValue(record.value(), NucleoData.class);
              if (eventHandler.getChainToMethod().containsKey(record.topic())) {
                Object[] methodData = eventHandler.getChainToMethod().get(record.topic());
                Object obj;
                if(methodData[0] instanceof Class) {
                  Class clazz = (Class) methodData[0];
                  obj = clazz.newInstance();
                }else{
                  obj = methodData[0];
                }
                Method method = (Method) methodData[1];
                NucleoData returnData = (NucleoData) method.invoke(obj, data);
                queue.add(new Object[]{ "nucleo.client."+returnData.getOrigin(), returnData });
              }else if(responders.containsKey(data.getUuid().toString())){
                responders.get(data.getUuid().toString()).run(data);
              }else{
                System.out.println("topic not found");
              }
            }catch (Exception e){
              e.printStackTrace();
            }
            System.out.println(topic+" Record Key " + record.key());
            System.out.println(topic+" Record value " + record.value());
            System.out.println(topic+" Record partition " + record.partition());
            System.out.println(topic+" Record offset " + record.offset());
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
}
