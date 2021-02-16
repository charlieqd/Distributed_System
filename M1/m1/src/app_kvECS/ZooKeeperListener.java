package app_kvECS;

import org.apache.zookeeper.WatchedEvent;

public interface ZooKeeperListener {
    void handleZooKeeperEvent(WatchedEvent event);
}
