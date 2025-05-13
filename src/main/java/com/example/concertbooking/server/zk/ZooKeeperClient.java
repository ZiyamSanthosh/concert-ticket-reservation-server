package com.example.concertbooking.server.zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class ZooKeeperClient {

    private static final String ZK_SERVER = "localhost:2181";
    private static final int SESSION_TIMEOUT = 3000;

    private final ZooKeeper zk;

    public ZooKeeperClient() throws IOException, InterruptedException {
        CountDownLatch connectedSignal = new CountDownLatch(1);
        zk = new ZooKeeper(ZK_SERVER, SESSION_TIMEOUT, event -> {
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                connectedSignal.countDown();
            }
        });
        connectedSignal.await();
    }

    public ZooKeeper getZooKeeper() {
        return zk;
    }

    public void createZNodeIfNotExists(String path, byte[] data, CreateMode mode) throws KeeperException, InterruptedException {
        try {
            Stat stat = zk.exists(path, false);
            if (stat == null) {
                zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, mode);
            }
        } catch (KeeperException.NodeExistsException e) {
            // Expected in concurrent setups: path already created
        }
    }

    public void createPersistentPathIfNeeded(String path) throws KeeperException, InterruptedException {
        createZNodeIfNotExists(path, new byte[0], CreateMode.PERSISTENT);
    }

    public void close() throws InterruptedException {
        zk.close();
    }
}
