package com.example.concertbooking.server.zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class DistributedLock {

    private final ZooKeeper zk;
    private final String lockPath;
    private String currentZnode;

    public DistributedLock(ZooKeeperClient zkClient, String concertId) throws KeeperException, InterruptedException {
        this.zk = zkClient.getZooKeeper();
        this.lockPath = "/concert_locks/" + concertId;

        // Ensure root path exists
        zkClient.createPersistentPathIfNeeded("/concert_locks");
        zkClient.createPersistentPathIfNeeded(lockPath);
    }

    public void acquire() throws KeeperException, InterruptedException {
        currentZnode = zk.create(lockPath + "/lock_",
                new byte[0],
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL_SEQUENTIAL);

        while (true) {
            List<String> children = zk.getChildren(lockPath, false);
            Collections.sort(children);

            String lowestZnode = children.get(0);
            String currentZnodeName = currentZnode.substring(currentZnode.lastIndexOf('/') + 1);

            if (lowestZnode.equals(currentZnodeName)) {
                // Acquired lock
                return;
            }

            // Watch previous znode
            int index = children.indexOf(currentZnodeName);
            String previousZnode = children.get(index - 1);
            final CountDownLatch latch = new CountDownLatch(1);

            Stat stat = zk.exists(lockPath + "/" + previousZnode, event -> {
                if (event.getType() == Watcher.Event.EventType.NodeDeleted) {
                    latch.countDown();
                }
            });

            if (stat != null) {
                latch.await();
            }
        }
    }

    public void release() throws KeeperException, InterruptedException {
        if (currentZnode != null) {
            zk.delete(currentZnode, -1);
        }
    }
}
