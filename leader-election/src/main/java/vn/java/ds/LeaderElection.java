package vn.java.ds;

import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class LeaderElection implements Watcher {
    private static final String DEFAULT_ZOOKEEPER_ADDRESS = "192.168.1.6:2181";
    private static final int DEFAULT_ZOOKEEPER_SESSION_TIMEOUT = 5000;
    private static final String ELECTION_NAMESPACE = "/election";
    private final ZooKeeper zooKeeper;
    private final AtomicBoolean isRunning;
    private String currentZNodeName;

    public LeaderElection() throws IOException {
        this.zooKeeper = new ZooKeeper(this.getZookeeperAddress(), this.getZookeeperSessionTimeout(), this);
        this.isRunning = new AtomicBoolean(true);
    }

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        LeaderElection leaderElection = new LeaderElection();
        leaderElection.volunteerForLeadership();
        leaderElection.reElectLeader();
        leaderElection.run();
        leaderElection.close();
    }

    private void run() throws InterruptedException {
        synchronized (zooKeeper) {
            while (isRunning.get()) {
                this.zooKeeper.wait();
            }
        }
    }

    public void close() throws InterruptedException {
        this.zooKeeper.close();
    }

    public void volunteerForLeadership() throws InterruptedException, KeeperException {
        String zNodePrefix = ELECTION_NAMESPACE + "/c_";
        String zNodeFullPath = zooKeeper.create(zNodePrefix, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        log.info("ZNodeName: " + zNodeFullPath);
        this.currentZNodeName = zNodeFullPath.replace(ELECTION_NAMESPACE + "/", "");
    }

    public void electLeader() throws InterruptedException, KeeperException {
        List<String> children = zooKeeper.getChildren(ELECTION_NAMESPACE, false);
        Collections.sort(children);
        final String smallestChildNode = children.get(0);
        if (smallestChildNode.equals(this.currentZNodeName)) {
            log.warn("I'm the Leader ");
            return;
        }

        log.warn("I'm not the Leader, " + smallestChildNode + " is the Leader");
    }

    public void reElectLeader() throws InterruptedException, KeeperException {
        Stat predecessorStat = null;
        String predecessorZNodeName = "";
        while (Objects.isNull(predecessorStat)) {
            List<String> children = zooKeeper.getChildren(ELECTION_NAMESPACE, false);
            Collections.sort(children);
            final String smallestChildNode = children.get(0);

            if (smallestChildNode.equals(this.currentZNodeName)) {
                log.warn("I'm the Leader ");
                return;
            } else {
                log.warn("I'm not the Leader, " + smallestChildNode + " is the Leader");
                int predecessorIndex = Collections.binarySearch(children, currentZNodeName) - 1;
                predecessorZNodeName = children.get(predecessorIndex);
                predecessorStat = zooKeeper.exists(ELECTION_NAMESPACE + "/" + predecessorZNodeName, this);
            }
        }

        log.warn("Watching ZNode: " + predecessorZNodeName);
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        switch (Objects.requireNonNull(watchedEvent.getType())) {
            case None:
                if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                    log.info("Successfully connected to Zookeeper");
                } else {
                    synchronized (zooKeeper) {
                        this.isRunning.set(false);
                        zooKeeper.notifyAll();
                        log.warn("Disconnected from Zookeeper");
                    }
                }

                break;
            case NodeDeleted:
                try {
                    reElectLeader();
                } catch (InterruptedException | KeeperException ex) {
                    log.error("ReElectLeader failure", ex);
                    Thread.currentThread().interrupt();
                }

                break;
            default:
                break;
        }
    }

    public String getZookeeperAddress() {
        return System.getProperty("zookeeper_address", DEFAULT_ZOOKEEPER_ADDRESS);
    }

    public int getZookeeperSessionTimeout() {
        try {
            return Integer.parseInt(System.getProperty("zookeeper_session_timeout"));
        } catch (NumberFormatException ex) {
            log.debug("ZookeeperSessionTimeout not configured. Then use default value!");
        }

        return DEFAULT_ZOOKEEPER_SESSION_TIMEOUT;
    }
}
