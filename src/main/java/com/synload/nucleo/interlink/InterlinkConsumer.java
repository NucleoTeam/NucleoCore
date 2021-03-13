package com.synload.nucleo.interlink;

import com.synload.nucleo.chain.path.SingularRun;
import com.synload.nucleo.data.NucleoData;
import com.synload.nucleo.zookeeper.ZooKeeperLeadershipClient;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

public class InterlinkConsumer implements Runnable{
    protected static final Logger logger = LoggerFactory.getLogger(InterlinkConsumer.class);
    Set<String> topics = new HashSet<>();
    InterlinkHandler interlinkHandler;
    KafkaConsumer<Long, NucleoData> consumer = null;
    boolean listening = false;
    boolean broadcast = false;
    Thread thread;
    public InterlinkConsumer(Properties props, InterlinkHandler interlinkHandler) {
        consumer = new KafkaConsumer<>(props);
        this.interlinkHandler = interlinkHandler;
    }
    public InterlinkConsumer(Properties props, InterlinkHandler interlinkHandler, boolean broadcast) {
        consumer = new KafkaConsumer<>(props);
        this.interlinkHandler = interlinkHandler;
        this.broadcast = broadcast;
    }

    @Override
    public void run() {
        try {
            while(!Thread.interrupted()) {
                ConsumerRecords<Long, NucleoData> records = consumer.poll(Duration.ofMillis(100));
                records.forEach(c->{
                    interlinkHandler.handleMessage(c.value());
                    //logger.info("received "+((SingularRun)c.value().getChainExecution().getCurrent()).getChain());
                });
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            consumer.unsubscribe();
            consumer.close();
        }
    }
    public void start(){
        InterlinkConsumer self = this;
        consumer.subscribe(topics, new ConsumerRebalanceListener() {
            private List<String> formatPartitions(Collection<TopicPartition> partitions) {
                return partitions.stream().map(topicPartition ->
                    String.format("topic: %s, partition: %s", topicPartition.topic(), topicPartition.partition()))
                    .collect(Collectors.toList());
            }
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                System.out.printf("onPartitionsRevoked - partitions: %s%n", formatPartitions(partitions));
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                System.out.printf("onPartitionsAssigned - partitions: %s%n", formatPartitions(partitions));
                listening = true;
            }
        });
        thread = new Thread(self);
        thread.start();
    }
    public void close(){
        consumer.unsubscribe();
        thread.interrupt();
        consumer.close();
    }

    public Set<String> getTopics() {
        return topics;
    }

    public void setTopics(Set<String> topics) {
        this.topics = topics;
    }

    public KafkaConsumer<Long, NucleoData> getConsumer() {
        return consumer;
    }

    public void setConsumer(KafkaConsumer<Long, NucleoData> consumer) {
        this.consumer = consumer;
    }

    public boolean isListening() {
        return listening;
    }

    public void setListening(boolean listening) {
        this.listening = listening;
    }
}
