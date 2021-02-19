package app_kvECS;

import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ZooKeeperService {

    private String url;

    private static Logger logger = Logger.getRootLogger();

    private final int DEFAULT_TIMEOUT = 2000;

    private ZooKeeper zooKeeper;

    private final List<ZooKeeperListener> listeners = new ArrayList<>();

    private class ProcessNodeWatcher implements Watcher {

        @Override
        public void process(WatchedEvent event) {
            synchronized (listeners) {
                for (ZooKeeperListener l : listeners) {
                    l.handleZooKeeperEvent(event);
                }
            }
        }
    }


    public ZooKeeperService(String url) throws
            IOException {
        this.url = url;
        zooKeeper = new ZooKeeper(url,
                DEFAULT_TIMEOUT, new ProcessNodeWatcher());
    }

    public String getURL() {
        return url;
    }

    public String createNode(final String node,
                             byte[] data,
                             final boolean ephemeral) throws Exception {
        String createdNodePath = null;
        try {
            final Stat nodeStat = zooKeeper.exists(node, false);

            CreateMode mode =
                    ephemeral ? CreateMode.EPHEMERAL : CreateMode.PERSISTENT;
            if (nodeStat == null) {
                createdNodePath = zooKeeper.create(node, data,
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, mode);
            }
        } catch (KeeperException | InterruptedException e) {
            throw new Exception(e);
        }

        return createdNodePath;
    }

    public boolean watchNode(final String node, final boolean watch) throws
            Exception {
        boolean watched = false;
        try {
            final Stat nodeStat = zooKeeper.exists(node, watch);

            if (nodeStat != null) {
                watched = true;
            }
        } catch (KeeperException | InterruptedException e) {
            throw new Exception(e);
        }

        return watched;
    }

    public List<String> getChildren(final String root, boolean watch) throws
            Exception {
        try {
            return zooKeeper.getChildren(root, watch);
        } catch (KeeperException | InterruptedException e) {
            throw new Exception(e);
        }
    }

    public void removeNode(final String node) throws Exception {
        try {
            zooKeeper.delete(node, -1);
        } catch (KeeperException | InterruptedException e) {
            throw new Exception(e);
        }
    }

    public void addListener(ZooKeeperListener listener) {
        synchronized (listeners) {
            listeners.add(listener);
        }
    }

}
