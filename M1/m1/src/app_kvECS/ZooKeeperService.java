package app_kvECS;

import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ZooKeeperService {

    private String host;

    private int port;

    private static Logger logger = Logger.getRootLogger();

    private final int DEFAULT_TIMEOUT = 2000;

    private ZooKeeper zooKeeper;

    private List<ZooKeeperListener> listeners = new ArrayList<>();

    public class ProcessNodeWatcher implements Watcher {

        @Override
        public void process(WatchedEvent event) {
            for (ZooKeeperListener l : listeners) {
                l.handleZooKeeperEvent(event);
            }
        }
    }


    public ZooKeeperService(String host, int port,
                            final ProcessNodeWatcher processNodeWatcher) throws
            IOException {
        this.host = host;
        this.port = port;
        String connectString = String.format("%s:%d", host, port);
        zooKeeper = new ZooKeeper(connectString,
                DEFAULT_TIMEOUT, processNodeWatcher);
    }


    public String createNode(final String node, final boolean watch,
                             byte[] data,
                             final boolean ephemeral) {
        String createdNodePath = null;
        try {
            final Stat nodeStat = zooKeeper.exists(node, watch);

            CreateMode mode = ephemeral ? CreateMode.EPHEMERAL : CreateMode.PERSISTENT;
            if (nodeStat == null) {
                createdNodePath = zooKeeper.create(node, data,
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            }
        } catch (KeeperException | InterruptedException e) {
            throw new IllegalStateException(e);
        }

        return createdNodePath;
    }

    public boolean watchNode(final String node, final boolean watch) {
        boolean watched = false;
        try {
            final Stat nodeStat = zooKeeper.exists(node, watch);

            if (nodeStat != null) {
                watched = true;
            }
        } catch (KeeperException | InterruptedException e) {
            throw new IllegalArgumentException(e);
        }

        return watched;
    }

    public List<String> watchChildren(final String root) {
        try {
            return zooKeeper.getChildren(root, true);
        } catch (KeeperException | InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }


    public void removeNode(final String node) {
        try {
            zooKeeper.delete(node, -1);
        } catch (KeeperException | InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    public void addListener(ZooKeeperListener listener) {
        listeners.add(listener);
    }

}
