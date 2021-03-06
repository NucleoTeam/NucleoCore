package com.synload.nucleo.interlink;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.synload.nucleo.data.FindLoop;
import com.synload.nucleo.data.NucleoData;
import org.apache.commons.lang3.SerializationException;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.IntrospectionException;
import java.lang.reflect.InvocationTargetException;
import java.util.Properties;
import java.util.Stack;
import java.util.concurrent.ExecutionException;

public class InterlinkProducer {
    Properties props;

    private static KafkaProducer<Long, NucleoData> producer=null;
    @JsonIgnore
    protected static final Logger logger = LoggerFactory.getLogger(InterlinkProducer.class);

    public InterlinkProducer(Properties props) {
        producer = new KafkaProducer<>(props);
    }

    boolean send(String topicName, NucleoData data) {

        long[] offsets = new long[2];
        RecordMetadata outMetadata;

        try {
            long time = System.currentTimeMillis();
            producer.send(new ProducerRecord<>(topicName, time, data), (metadata, e)->{
                if(e!=null) e.printStackTrace();
            });
        } catch (SerializationException e) {
            e.printStackTrace();
            try {
                new FindLoop().find(data, "\t");
            } catch (IntrospectionException ex) {
                ex.printStackTrace();
            } catch (InvocationTargetException ex) {
                ex.printStackTrace();
            } catch (IllegalAccessException ex) {
                ex.printStackTrace();
            }
        }
        return false;
    }
}
