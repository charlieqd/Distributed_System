package server;

import app_kvServer.KVServer;
import client.ServerConnection;
import ecs.MoveDataArgs;
import org.apache.log4j.Logger;
import server.KVStorageDelta.Value;
import shared.*;
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

    private static class ReplicaState {
        ServerConnection connection;
        String name;

        private Integer lastSyncLogicalTime = null;

        public ReplicaState(IProtocol protocol,
                            ISerializer<KVMessage> serializer,
                            ECSNode node) {
            this.connection = new ServerConnection(protocol, serializer,
                    node.getNodeHost(), node.getNodePort());
            this.name = node.getNodeName();
        }

        public Integer getLastSyncLogicalTime() {
            if (!connection.isConnectionValid()) {
                return null;
            }
            return lastSyncLogicalTime;
        }

        public void reset() {
            lastSyncLogicalTime = null;
        }
    }

    private static final long UPDATE_MILLIS = 1000;

    private static Logger logger = Logger.getRootLogger();

    private IProtocol protocol;
    private ISerializer<KVMessage> serializer;
    private final KVServer server;
    private final IKVStorage storage;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean enabled = new AtomicBoolean(false);
    private final AtomicBoolean replicating = new AtomicBoolean(false);

    private List<KVStorageDelta> deltas = new ArrayList<>();
    /**
     * Map from node name to replica state.
     */
    private Map<String, ReplicaState> replicaStatesMap = new HashMap<>();
    private List<ReplicaState> replicaStates = new ArrayList<>();

    private String selfRangeStart = null;
    private String selfRangeEnd = null;

    private int nextReplica = 0;

    private int logicalTime = 0;

    public Replicator(IProtocol protocol,
                      ISerializer<KVMessage> serializer,
                      KVServer server,
                      IKVStorage storage) {
        this.protocol = protocol;
        this.serializer = serializer;
        this.server = server;
        this.storage = storage;
    }

    @Override
    public void run() {
        while (running.get()) {
            if (enabled.get()) {
                checkReplicaStates();
                replicate();
            }

            try {
                Thread.sleep(UPDATE_MILLIS);
            } catch (InterruptedException e) {
                logger.error(e);
                return;
            }
        }
    }

    private void checkReplicaStates() {
        Metadata metadata = server.metadata.get();
        HashMap<String, ECSNode> newReplicaNodes = new HashMap<>();
        if (metadata != null) {
            // TODO: Can optimize this part to not use linear search
            ECSNode selfNode = metadata
                    .getServerByName(server.getNodeName());
            if (selfNode != null) {
                // Detect self hash range
                ECSNode predecessor = metadata.getPredecessor(selfNode);
                String newSelfRangeStart = predecessor.getPosition();
                String newSelfRangeEnd = selfNode.getPosition();
                if (!newSelfRangeStart
                        .equals(selfRangeStart) || !newSelfRangeEnd
                        .equals(selfRangeEnd)) {
                    // If hash range changed, must perform full replication
                    for (ReplicaState state : replicaStatesMap.values()) {
                        state.reset();
                    }

                    selfRangeStart = newSelfRangeStart;
                    selfRangeEnd = newSelfRangeEnd;

                    storage.startNextDeltaRecording(logicalTime++,
                            selfRangeStart,
                            selfRangeEnd);
                    deltas.clear();
                }

                // Detect replicas
                ECSNode node = selfNode;
                node = metadata.getSuccessor(node);
                newReplicaNodes.put(node.getNodeName(), node);
                node = metadata.getSuccessor(node);
                newReplicaNodes.put(node.getNodeName(), node);
                // The node itself cannot be a replica
                newReplicaNodes.remove(selfNode.getNodeName());
            }
        }

        // Remove existing replica state if no longer a replica
        ArrayList<String> namesToRemove = new ArrayList<>();
        for (Map.Entry<String, ReplicaState> entry : replicaStatesMap
                .entrySet()) {
            if (!newReplicaNodes.containsKey(entry.getKey())) {
                namesToRemove.add(entry.getKey());
            }
        }
        for (String name : namesToRemove) {
            ReplicaState state = replicaStatesMap.get(name);
            state.connection.disconnect(true);
            replicaStatesMap.remove(name);
            replicaStates.remove(state);
        }

        // Add new replica state if not already exist
        for (String name : newReplicaNodes.keySet()) {
            if (!replicaStatesMap.containsKey(name)) {
                ReplicaState state = new ReplicaState(protocol, serializer,
                        newReplicaNodes.get(name));
                replicaStatesMap.put(name, state);
                replicaStates.add(state);
            }
        }
    }

    private void replicate() {
        replicating.set(true);
        try {
            if (replicaStates.size() == 0) {
                return;
            }

            if (nextReplica >= replicaStates.size()) {
                nextReplica = 0;
            }

            ReplicaState state = replicaStates.get(nextReplica);
            nextReplica++;

            Integer lastSyncLogicalTime = state.getLastSyncLogicalTime();
            // Note that if connection is not valid, this will be null, which
            // forces a full replication.

            boolean incrementalAllowed = false;
            int deltaIndex = -1;

            if (lastSyncLogicalTime != null) {
                for (int i = 0; i < deltas.size(); ++i) {
                    KVStorageDelta delta = deltas.get(i);
                    if (delta.getLogicalTime() == lastSyncLogicalTime) {
                        deltaIndex = i;
                        break;
                    }
                }
                if (deltaIndex != -1) {
                    incrementalAllowed = true;
                }
            }

            if (!state.connection.isConnectionValid()) {
                // Attempt to connect
                state.connection.disconnect(true);
                try {
                    state.connection.connect();
                } catch (Exception e) {
                    logger.error(String.format(
                            "Unable to connect to replica %s",
                            state.name), e);
                }
            }

            // Attempt incremental replication
            if (incrementalAllowed) {
                KVStorageDelta newDelta = storage
                        .startNextDeltaRecording(logicalTime++, selfRangeStart,
                                selfRangeEnd);
                if (newDelta == null) {
                    // Fall-back to full replication
                    deltas.clear();
                    incrementalAllowed = false;
                } else {
                    deltas.add(newDelta);
                }

                if (incrementalAllowed) {
                    for (int i = deltaIndex; i < deltas.size(); ++i) {
                        KVStorageDelta delta = deltas.get(i);
                        if (!enabled.get()) {
                            // Skip this replica
                            break;
                        }
                        if (!state.connection.isConnectionValid()) {
                            // Skip this replica
                            state.reset();
                            break;
                        }
                        boolean success = incrementalReplication(delta,
                                state.connection);
                        if (success) {
                            if (i == deltas.size() - 1) {
                                state.lastSyncLogicalTime = storage
                                        .getCurrentDeltaLogicalTime();
                            } else {
                                state.lastSyncLogicalTime = deltas.get(i + 1)
                                        .getLogicalTime();
                            }
                        } else {
                            // Skip this replica
                            // We can re-try this, since incremental replication
                            // is idempotent
                            break;
                        }
                    }
                }
            }

            // Attempt full replication
            if (!incrementalAllowed) {
                server.lockSelfWrite();
                KVStorageDelta newDelta = storage
                        .startNextDeltaRecording(logicalTime++, selfRangeStart,
                                selfRangeEnd);
                if (newDelta == null) {
                    deltas.clear();
                } else {
                    deltas.add(newDelta);
                }
                boolean success = fullReplication(selfRangeStart, selfRangeEnd,
                        state.connection);
                if (success) {
                    state.lastSyncLogicalTime = storage
                            .getCurrentDeltaLogicalTime();
                } else {
                    // Skip this replica
                    // We can re-try this, since full replication is idempotent
                }
                server.unlockSelfWrite();
            }

            // TODO: remove deltas that will never be used, maybe at start?
            throw new Error("Not implemented");
        } finally {
            replicating.set(false);
        }
    }

    public void startReplication() {
        enabled.set(true);
    }

    public void stopReplication() {
        enabled.set(false);
        while (replicating.get()) {
            // Wait until ongoing replication stops
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                logger.error(e);
            }
        }
    }

    public void shutdown() {
        stopReplication();
        running.set(false);
    }

    private boolean fullReplication(String rangeStart,
                                    String rangeEnd,
                                    ServerConnection targetConnection) {
        return deleteReplicaData(rangeStart, rangeEnd,
                targetConnection) && copyDataTo(rangeStart, rangeEnd,
                targetConnection);
    }

    private boolean incrementalReplication(KVStorageDelta delta,
                                           ServerConnection targetConnection) {
        for (Map.Entry<String, Value> entry : delta.getEntrySet()) {
            if (!enabled.get()) {
                return false;
            }
            String key = entry.getKey();
            Value value = entry.getValue();
            Set<KVMessage.StatusType> status_Set = new HashSet<KVMessage.StatusType>(
                    Arrays.asList(KVMessage.StatusType.PUT_UPDATE,
                            KVMessage.StatusType.PUT_SUCCESS));
            boolean result = sendCommandToNode(targetConnection,
                    new KVMessageImpl(key, value.get(),
                            KVMessage.StatusType.ECS_PUT), status_Set);
            if (!result) {
                logger.error("Incrementally Replication Failed");
                return false;
            }
        }
        return true;
    }

    private boolean deleteReplicaData(String rangeStart, String rangeEnd,
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
            if (!enabled.get()) {
                return false;
            }

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
                        String.format(
                                "Unable to send message to node %s:%d",
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
