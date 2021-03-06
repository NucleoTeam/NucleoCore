package com.synload.nucleo.interlink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.synload.nucleo.NucleoMesh;
import com.synload.nucleo.data.NucleoData;
import com.synload.nucleo.utils.ObjectDeserializer;
import com.synload.nucleo.utils.ObjectSerializer;
import org.apache.commons.collections.list.TreeList;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;
import java.util.stream.Collectors;

public class InterlinkManager {
    protected static final Logger logger = LoggerFactory.getLogger(InterlinkManager.class);
    NucleoMesh mesh;
    Properties consumerProps = new Properties();
    Properties producerProps = new Properties();
    Properties brodcastProps = new Properties();
    InterlinkConsumer consumer;
    InterlinkConsumer broadcast;
    InterlinkProducer producer;
    ObjectMapper mapper = new ObjectMapper();

    public InterlinkManager(NucleoMesh mesh, String kafkaServers){
        this.mesh = mesh;
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");


        consumerProps.putAll(properties);
        consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
        consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, mesh.getServiceName());
        consumerProps.setProperty(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "78643200");
        consumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ObjectDeserializer.class.getName());

        producerProps.putAll(properties);
        producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
        producerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        producerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ObjectSerializer.class.getName());
        producer = new InterlinkProducer(producerProps);
        consumer = new InterlinkConsumer(consumerProps, (data)->{
            mesh.getEventHandler().callInterlinkEvent(InterlinkEventType.RECEIVE_TOPIC, mesh, data);
            mesh.getHub().handle(mesh.getHub(), data);
        });
        brodcastProps.putAll(consumerProps);

        brodcastProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, mesh.getUniqueName());
        broadcast = new InterlinkConsumer(brodcastProps, (data)->{
            mesh.getEventHandler().callInterlinkEvent(InterlinkEventType.RECEIVE_TOPIC, mesh, data);
            mesh.getHub().handle(mesh.getHub(), data);
        });
    }
    public void subscribeBroadcasts(String... topics) {
        broadcast.getTopics().addAll(Arrays.asList(topics).stream().map(c->"broadcast_"+c).collect(Collectors.toList()));
    }
    public void subscribeBroadcasts(Collection<String> topics) {
        consumer.getTopics().addAll(topics.stream().map(c->"broadcast_"+c).collect(Collectors.toList()));
    }

    public void subscribeLeader(String topic) {
        InterlinkConsumer ilc = new InterlinkConsumer(consumerProps, (data)->{
            mesh.getEventHandler().callInterlinkEvent(InterlinkEventType.RECEIVE_TOPIC, mesh, data);
            mesh.getHub().handle(mesh.getHub(), data);
        }, false);
        ilc.getTopics().add("leader_"+topic);
        ilc.start();

    }
    public void start(){
        consumer.start();
        broadcast.start();
    }
    public void subscribeLeader(String topic, int leaders) {
        InterlinkConsumer ilc = new InterlinkConsumer(consumerProps, (data)->{
            mesh.getEventHandler().callInterlinkEvent(InterlinkEventType.RECEIVE_TOPIC, mesh, data);
            mesh.getHub().handle(mesh.getHub(), data);
        }, false);
        ilc.getTopics().add("leader_"+topic);
        ilc.start();
    }

    public void subscribe(String... topics) {
        consumer.getTopics().addAll(Arrays.asList(topics));
    }

    public void subscribe(Collection<String> topics) {
        consumer.getTopics().addAll(topics);
    }

    public void unsubscribeLeader(String topic) {
        unsubscribe("leader_"+topic);
    }
    public void unsubscribeBroadcast(String... topics){
        Set<String> topicsToRemove = new HashSet<>(Arrays.asList(topics));
        Set<String> topicsLeft = broadcast.getTopics().stream().filter(k->!topicsToRemove.contains(k)).collect(Collectors.toSet());
        broadcast.setTopics(topicsLeft);
        broadcast.getConsumer().unsubscribe();
        broadcast.getConsumer().subscribe(topicsLeft);
    }

    public void unsubscribe(String... topics){
        Set<String> topicsToRemove = new HashSet<>(Arrays.asList(topics));
        Set<String> topicsLeft = consumer.getTopics().stream().filter(k->!topicsToRemove.contains(k)).collect(Collectors.toSet());
        consumer.setTopics(topicsLeft);
        consumer.getConsumer().unsubscribe();
        consumer.getConsumer().subscribe(topicsLeft);
    }


    public void broadcast(String topic, NucleoData data){
        send("broadcast_"+topic, data);
    }

    public void leader(String topic, NucleoData data){
        send("leader_"+topic, data);
    }

    public void send(String topic, NucleoData data){
        mesh.getEventHandler().callInterlinkEvent(InterlinkEventType.SEND_TOPIC, mesh, topic, data);
        producer.send(topic, data);
    }

    public void close(){
        this.consumer.close();
        this.broadcast.close();
    }

    public NucleoMesh getMesh() {
        return mesh;
    }

    public void setMesh(NucleoMesh mesh) {
        this.mesh = mesh;
    }

}
