package server;

import app_kvServer.KVServer;
import client.ServerConnection;
import ecs.MoveDataArgs;
import shared.messages.KVMessage;
import shared.messages.KVMessageImpl;

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
        try {
            int request_id = targetConnection
                    .sendRequest(new KVMessageImpl(null, null,
                            null, KVMessage.StatusType.ECS_DELETE_DATA,
                            new MoveDataArgs(rangeStart, rangeEnd, null, 0)));

        } catch (Exception e) {
            logger.error(String.format(
                    "Failed to send put request (key: %s) to target server",
                    key));
            return false;
        }
    }

    private void incrementalReplication(KVStorageDelta delta,
                                        ServerConnection targetConnection) {
        throw new Error("Not implemented");
    }
}
