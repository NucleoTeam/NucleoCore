package com.synload.nucleo.zookeeper;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class Connection {
    private CuratorFramework zooClient;
    CountDownLatch connectionLatch = new CountDownLatch(1);

    // ...

    public CuratorFramework connect(String host) throws InterruptedException{
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(4000, 10);
        System.out.println("Connecting to zookeeper: "+host);
        zooClient = CuratorFrameworkFactory.newClient(host, retryPolicy);
        zooClient.start();
        zooClient.blockUntilConnected();
        return zooClient;
    }

    public void close() throws InterruptedException {
        zooClient.close();
    }
}
