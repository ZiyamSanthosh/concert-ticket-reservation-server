package com.example.concertbooking.server.zk;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.Collections;
import java.util.List;

public class LeaderElection {

    private final ZooKeeper zooKeeper;
    private final String electionPath = "/leader_election";
    private final String leaderPath = "/leader";
    private String currentZnodeName;
    private final String nodeAddress;
    private boolean isLeader = false;

    public LeaderElection(ZooKeeperClient zkClient, int port) throws Exception {
        this.zooKeeper = zkClient.getZooKeeper();
        this.nodeAddress = "localhost:" + port;

        // Ensure parent path exists
        if (zooKeeper.exists(electionPath, false) == null) {
            zooKeeper.create(electionPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        // Create ephemeral sequential znode
        String znodeFullPath = zooKeeper.create(
                electionPath + "/node-",
                nodeAddress.getBytes(),  // store address for clarity
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL_SEQUENTIAL
        );

        this.currentZnodeName = znodeFullPath.replace(electionPath + "/", "");
        attemptLeadership();
    }

    private void attemptLeadership() throws Exception {
        List<String> children = zooKeeper.getChildren(electionPath, false);
        Collections.sort(children);
        int index = children.indexOf(currentZnodeName);

        if (index == 0) {
            this.isLeader = true;
            System.out.println("This node is the LEADER: " + currentZnodeName);

            // Update the leader znode
            Stat stat = zooKeeper.exists(leaderPath, false);
            byte[] data = nodeAddress.getBytes();

            if (stat == null) {
                zooKeeper.create(leaderPath, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            } else {
                zooKeeper.setData(leaderPath, data, -1);
            }
        } else {
            String watchNode = children.get(index - 1);
            zooKeeper.exists(electionPath + "/" + watchNode, event -> {
                if (event.getType() == Watcher.Event.EventType.NodeDeleted) {
                    try {
                        attemptLeadership();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });

            System.out.println("FOLLOWER " + currentZnodeName + " watching " + watchNode);
        }
    }

    public boolean isLeader() {
        return isLeader;
    }

    public String getLeaderAddress() throws Exception {
        Stat stat = zooKeeper.exists(leaderPath, false);
        if (stat != null) {
            return new String(zooKeeper.getData(leaderPath, false, null));
        }
        return null;
    }

    public String getCurrentZnodeName() {
        return currentZnodeName;
    }
}
