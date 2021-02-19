package app_kvECS;

import org.apache.zookeeper.WatchedEvent;

public interface ZooKeeperListener {
    /**
     * NOTE: Do not add/remove listener inside this handler. Keep in mind that
     * this handler may be called from a different thread.
     */
    void handleZooKeeperEvent(WatchedEvent event);
}
