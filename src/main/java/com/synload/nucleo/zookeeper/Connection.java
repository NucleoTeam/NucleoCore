package com.synload.nucleo.zookeeper;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Connection {
    private CuratorFramework zooClient;
    protected static final Logger logger = LoggerFactory.getLogger(Connection.class);

    // ...

    public CuratorFramework connect(String host) throws InterruptedException{
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(30000, 100000);
        logger.info("Connecting to zookeeper: "+host);
        zooClient = CuratorFrameworkFactory.newClient(host, retryPolicy);
        zooClient.start();
        zooClient.blockUntilConnected();
        return zooClient;
    }

    public void close() throws InterruptedException {
        zooClient.close();
    }
}
