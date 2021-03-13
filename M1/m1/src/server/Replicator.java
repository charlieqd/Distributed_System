package server;

import app_kvServer.KVServer;
import client.ServerConnection;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * NOTE: This class is technically not thread safe, however only one ECS will be
 * active, which effectively means there's no concurrent access to this object.
 */
public class Replicator extends Thread {

    private final KVServer server;

    private final AtomicBoolean enabled = new AtomicBoolean(false);

    public Replicator(KVServer server) {
        this.server = server;
    }

    @Override
    public void run() {

    }

    public void startReplication() {
        throw new Error("Not implemented");
    }

    public void stopReplication() {
        if (!enabled.get()) {
            return;
        }
        throw new Error("Not implemented");
    }

    private void fullReplication(String rangeStart,
                                 String rangeEnd,
                                 ServerConnection targetConnection) {
        throw new Error("Not implemented");
    }

    private void incrementalReplication(KVStorageDelta delta,
                                        ServerConnection targetConnection) {
        throw new Error("Not implemented");
    }
}
