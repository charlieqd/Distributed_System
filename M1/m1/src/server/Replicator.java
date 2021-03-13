package server;

import app_kvServer.KVServer;
import client.ServerConnection;
import ecs.MoveDataArgs;
import org.apache.log4j.Logger;
import shared.Util;
import shared.messages.KVMessage;
import shared.messages.KVMessageImpl;

import java.io.IOException;
import java.util.*;
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

    private boolean fullReplication(String rangeStart,
                                    String rangeEnd,
                                    ServerConnection targetConnection) {

        boolean success = deleteReplicateData(rangeStart, rangeEnd,
                targetConnection))
        success = success && copyDataTo(rangeStart,
                rangeEnd, targetConnection);
        return success;

    }

    private void incrementalReplication(KVStorageDelta delta,
                                        ServerConnection targetConnection) {
        throw new Error("Not implemented");
    }

    private boolean deleteReplicateData(String rangeStart, String rangeEnd,
                                        ServerConnection targetConnection) {
        KVMessage msg = new KVMessageImpl(null, null,
                null, KVMessage.StatusType.ECS_DELETE_DATA,
                new MoveDataArgs(rangeStart, rangeEnd, null, 0));
        return sendCommandToNode(targetConnection, msg,
                new HashSet<KVMessage.StatusType>(
                        Arrays.asList(KVMessage.StatusType.ECS_SUCCESS)));
    }

    public boolean copyDataTo(String hashRangeStart,
                              String hashRangeEnd,
                              ServerConnection targetConnection) {
        List<String> keys = null;
        try {
            keys = storage.getAllKeys(hashRangeStart, hashRangeEnd);
        } catch (IOException e) {
            logger.error(e);
            return false;
        }


        for (String key : keys) {
            String value;
            try {
                value = storage.get(key);
            } catch (IOException e) {
                logger.error("Internal server error: " +
                        Util.getStackTraceString(e));
                return false;
            }

            KVMessage msg = new KVMessageImpl(key, value,
                    KVMessage.StatusType.ECS_PUT);
            boolean success = sendCommandToNode(targetConnection, msg,
                    new HashSet<KVMessage.StatusType>(
                            Arrays.asList(KVMessage.StatusType.PUT_SUCCESS,
                                    KVMessage.StatusType.PUT_UPDATE)));
            if (!success) {
                logger.error(String.format(
                        "Failed to send put request (key: %s) to target server",
                        key));
                return false;
            }
        }

        logger.info("Successfully copy all data to target server.");
        return true;

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
