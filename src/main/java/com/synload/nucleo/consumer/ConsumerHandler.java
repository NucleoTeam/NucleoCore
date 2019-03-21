package com.synload.nucleo.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.*;

import java.util.*;
import java.util.regex.Pattern;

public class ConsumerHandler {
  private KafkaConsumer consumer;

  public ConsumerHandler(String bootstrap) {
    this.consumer = createConsumer(bootstrap);
  }

  private KafkaConsumer createConsumer(String bootstrap) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
    props.put(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    KafkaConsumer consumer = new KafkaConsumer(props);
    return consumer;
  }
  public void subscribe(String topic){
    System.out.println("Subscribed to topic " + topic);
    consumer.subscribe(Collections.singleton(topic));
  }

  public KafkaConsumer getConsumer() {
    return this.consumer;
  }

  public void setConsumer(KafkaConsumer consumer) {
    this.consumer = consumer;
  }
}