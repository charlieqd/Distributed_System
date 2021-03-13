package server;

import app_kvServer.KVServer;
import client.ServerConnection;
import server.KVStorageDelta.Value;
import shared.messages.KVMessage;
import shared.messages.KVMessageImpl;

import java.io.IOException;
import java.util.*;

import ecs.MoveDataArgs;
import org.apache.log4j.Logger;
import shared.messages.KVMessage;
import shared.messages.KVMessageImpl;

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
            int requestId = targetConnection
                    .sendRequest(new KVMessageImpl(null, null,
                            null, KVMessage.StatusType.ECS_DELETE_DATA,
                            new MoveDataArgs(rangeStart, rangeEnd, null, 0)));

            if (requestId == -1) {
                logger.error("Unable to delete replicate data");
                return false;
            }

            try {
                KVMessage resMessage = targetConnection
                        .receiveMessage(requestId);
                KVMessage.StatusType resStatus = resMessage.getStatus();
                if (resStatus == KVMessage.StatusType.ECS_SUCCESS) {
                    return true;
                } else {
                    logger.error(
                            "Failed to send data to next server: response status = " + resStatus
                                    .toString());
                    return false;
                }
            } catch (Exception e) {
                logger.error(
                        "Failed to receive ecs delete response from target server");
                return false;
            }
        } catch (Exception e) {
            logger.error("Unable to delete replicate data");
            return false;
        }
        deleteReplicateData(rangeStart, rangeEnd, targetConnection);
    }

    private void incrementalReplication(KVStorageDelta delta,
                                        ServerConnection targetConnection) {
        for (Map.Entry<String, Value> entry : delta.getEntrySet()) {
            String key = entry.getKey();
            Value value = entry.getValue();
            Set<KVMessage.StatusType> status_Set = new HashSet<KVMessage.StatusType>(Arrays.asList(KVMessage.StatusType.PUT_UPDATE, KVMessage.StatusType.PUT_SUCCESS));
            boolean result = sendCommandToNode(targetConnection, new KVMessageImpl(key, value.get(), KVMessage.StatusType.ECS_PUT), status_Set);
            if(!result){
                logger.error("Incrementally Replication Failed");
            }
        }
    }

    private boolean sendCommandToNode(ServerConnection targetConnection,
                                      KVMessage msg,
                                      Set<KVMessage.StatusType> successStatus) {
        try {
            int requestId = targetConnection
                    .sendRequest(msg);

            if (requestId == -1) {
                logger.error(
                        String.format("Unable to send message to node %s:%d",
                                targetConnection.getAddress(),
                                targetConnection.getPort()));
                return false;
            }

            try {
                KVMessage resMessage = targetConnection
                        .receiveMessage(requestId);
                KVMessage.StatusType resStatus = resMessage.getStatus();
                if (successStatus.contains(resStatus)) {
                    return true;
                } else {
                    logger.error(String.format(
                            "Node %s:%d responded with failure: " + resMessage
                                    .getValue(),
                            targetConnection.getAddress(),
                            targetConnection.getPort()));
                    return false;
                }
            } catch (Exception e) {
                logger.error(String.format(
                        "Failed to receive response from Node %s:%d",
                        targetConnection.getAddress(),
                        targetConnection.getPort()));
                return false;
            }
        } catch (Exception e) {
            logger.error(String.format(
                    "Failed to send command to Node %s:%d",
                    targetConnection.getAddress(),
                    targetConnection.getPort()));
            return false;
        }
    }
}
