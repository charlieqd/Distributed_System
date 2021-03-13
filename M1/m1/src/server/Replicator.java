package server;

import app_kvServer.KVServer;
import client.ServerConnection;
import server.KVStorageDelta.Value;
import shared.messages.KVMessage;
import shared.messages.KVMessageImpl;

import java.io.IOException;
import java.util.Map;
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
        // shiftshift search files cmd+o: search for class
        // loop through delta, connection push data
        // pretend ecs to delete
        for (Map.Entry<String, Value> entry : delta.getEntrySet()) {
            String key = entry.getKey();
            Value value = entry.getValue();
            int id = -1;
            try {
                id = targetConnection.sendRequest(key, value.get(), KVMessage.StatusType.PUT);
            } catch (IOException e) {
                targetConnection.disconnect();
                // throw error put failed
            }
            if (id == -1) {
                // throw error
            }
        }
        throw new Error("Not implemented");
    }
}
