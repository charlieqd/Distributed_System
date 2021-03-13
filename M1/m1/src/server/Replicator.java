package server;

import app_kvServer.KVServer;
import client.ServerConnection;
import server.KVStorageDelta.Value;
import shared.messages.KVMessage;
import shared.messages.KVMessageImpl;

import java.io.IOException;
import java.util.Map;
import ecs.MoveDataArgs;
import org.apache.log4j.Logger;
import shared.messages.KVMessage;
import shared.messages.KVMessageImpl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * NOTE: This class is technically not thread safe; however, only one ECS will
 * be active, which effectively means there's no concurrent access to this
 * object.
 */
public class Replicator extends Thread {

    private static final long UPDATE_MILLIES = 1000;

    private static Logger logger = Logger.getRootLogger();

    private final KVServer server;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean enabled = new AtomicBoolean(false);

    private List<KVStorageDelta> deltas = new ArrayList<>();

    public Replicator(KVServer server) {
        this.server = server;
    }

    @Override
    public void run() {
        while (running.get()) {

            try {
                Thread.sleep(UPDATE_MILLIES);
            } catch (InterruptedException e) {
                logger.error(e);
                return;
            }
        }
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

    public void shutdown() {
        running.set(false);
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
            logger.error("Unable to delete transferred data");
        }
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
