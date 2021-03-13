package server;

import app_kvServer.KVServer;
import client.ServerConnection;
import org.apache.log4j.Logger;
import shared.ECSNode;
import shared.IProtocol;
import shared.ISerializer;
import shared.Metadata;
import shared.messages.KVMessage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
                replicateStep();
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

    private void replicateStep() {
        if (replicaStates.size() == 0) {
            return;
        }

        if (nextReplica >= replicaStates.size()) {
            nextReplica = 0;
        }

        ReplicaState state = replicaStates.get(nextReplica);
        nextReplica++;

        Integer lastSyncLogicalTime = state.getLastSyncLogicalTime();

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

        KVStorageDelta newDelta = storage
                .startNextDeltaRecording(logicalTime++);
        if (newDelta != null) {
            deltas.add(newDelta);
        }
        if (incrementalAllowed) {

        }

        // TODO: remove deltas that will never be used, maybe at start?
        throw new Error("Not implemented");
    }

    public void startReplication() {
        enabled.set(true);
        throw new Error("Not implemented");
    }

    public void stopReplication() {
        if (!enabled.get()) {
            return;
        }
        enabled.set(false);
        while (replicating.get()) {
            // Wait until ongoing replication stops
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                logger.error(e);
            }
        }
        throw new Error("Not implemented");
    }

    public void shutdown() {
        stopReplication();
        running.set(false);
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
