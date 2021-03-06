package com.synload.nucleo.interlink;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;

import java.util.Arrays;
import java.util.Properties;

public class InterlinkKafkaAdmin {
    public InterlinkKafkaAdmin(Properties props) {
        Properties properties = new Properties();
        properties.putAll(props);
        Admin admin = Admin.create(properties);
        admin.deleteTopics(Arrays.asList("test"));
    }
}
