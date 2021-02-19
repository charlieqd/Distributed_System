package ecs;

import app_kvECS.ZooKeeperListener;
import app_kvECS.ZooKeeperService;
import client.ServerConnection;
import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import shared.ECSNode;
import shared.IProtocol;
import shared.ISerializer;
import shared.Metadata;
import shared.messages.KVMessage;
import shared.messages.KVMessageImpl;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;

/**
 * NOTE: This class it not thread-safe.
 */
public class ECSController implements ZooKeeperListener {
    // The following is guaranteed for each ServerConnection:
    // - Each ServerConnection will only be accessed by one thread at a time.
    //   This is done by setting each node status to LAUNCHED only after
    //   ServerConnection is connected, and no commands are sent unless node
    //   status is LAUNCHED.
    // - Exactly one sendRequest will be called before each receiveMessage.

    public static class NodeCommandException extends Exception {
        private boolean[] successes;

        public NodeCommandException(String message, boolean[] successes) {
            super(message);
            this.successes = successes;
        }

        public boolean[] getSuccesses() {
            return successes;
        }
    }

    public static final String ZOO_KEEPER_ROOT = "kvECS";

    public static final long LAUNCH_TIMEOUT_MS = 10000;

    public static final int DEFAULT_COMMAND_TIMEOUT_SECONDS = 30;
    public static final int MOVE_DATA_COMMAND_TIMEOUT_SECONDS = 300;

    // The server might need to delete data
    public static final int SHUTDOWN_COMMAND_TIMEOUT_SECONDS = 60;

    private static Logger logger = Logger.getRootLogger();

    private ArrayList<ECSNode> nodes;
    private ZooKeeperService zooKeeperService;
    private Map<String, ECSNodeState> nodeStates;

    public ECSController(IProtocol protocol,
                         ISerializer<KVMessage> serializer,
                         String configPath,
                         ZooKeeperService zooKeeperService) throws
            Exception {
        nodes = readConfig(configPath);
        nodeStates = new HashMap<>();
        for (ECSNode node : nodes) {
            nodeStates.put(node.getNodeName(),
                    new ECSNodeState(protocol, serializer, node));
        }

        this.zooKeeperService = zooKeeperService;
        zooKeeperService.addListener(this);

        // Create root directory in zookeeper
        try {
            this.zooKeeperService
                    .createNode(ZOO_KEEPER_ROOT, new byte[0], false);
        } catch (Exception e) {
            logger.error("Failed to create ZooKeeper node", e);
            throw e;
        }

        try {
            onZooKeeperChildrenChanged(getZooKeeperChildren(true));
        } catch (Exception e) {
            logger.error("Failed to watch ZooKeeper node", e);
            throw e;
        }
    }

    public boolean addNode(String cacheStrategy, int cacheSize) {
        List<ECSNode> availableToAdd = getNodesWithStatus(
                ECSNodeState.Status.NOT_LAUNCHED);
        if (availableToAdd.isEmpty()) {
            String msg = "No available nodes to add.";
            logger.error(msg);
            return false;
        }

        int randomNum = ThreadLocalRandom.current()
                .nextInt(0, availableToAdd.size());
        ECSNode node = availableToAdd.get(randomNum);
        ECSNodeState state = getNodeState(node);
        state.getStatus().set(ECSNodeState.Status.LAUNCHING);

        // Launch node

        Process proc;
        String script = String
                .format("invoke_server.sh %s %s %s %d %s %s",
                        node.getNodeHost(),
                        node.getNodePort(), cacheStrategy, cacheSize,
                        zooKeeperService.getURL(), node.getNodeName());

        Runtime run = Runtime.getRuntime();
        try {
            proc = run.exec(script);
        } catch (IOException e) {
            String msg = String
                    .format("Failed to add node, name: %s, host: %s, port: %s",
                            node.getNodeName(), node.getNodeHost(),
                            node.getNodePort());
            logger.error(msg, e);
            state.getStatus().set(ECSNodeState.Status.NOT_LAUNCHED);
            return false;
        }

        logger.info(
                String.format("Launching node: %s %s:%d", node.getNodeName(),
                        node.getNodeHost(), node.getNodePort()));

        // Wait for node launch

        try {
            getZooKeeperChildren(true);
        } catch (Exception e) {
            logger.error("Failed to watch ZooKeeper node", e);
        }

        long startMs = System.currentTimeMillis();
        while (state.getStatus().get() != ECSNodeState.Status.LAUNCHED &&
                System.currentTimeMillis() - startMs < LAUNCH_TIMEOUT_MS) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        if (state.getStatus().get() != ECSNodeState.Status.LAUNCHED) {
            // Timeout
            logger.error(String.format(
                    "Failed to launch ZooKeeper node %s: launch not detected before timeout",
                    node.getNodeName()));
            state.getStatus().set(ECSNodeState.Status.NOT_LAUNCHED);
            return false;
        }

        // Connect to node

        logger.info(String.format("Connecting to node %s",
                node.getNodeName()));

        try {
            state.getConnection().connect();
        } catch (Exception e) {
            logger.error(String.format("Failed to connect to node %s",
                    node.getNodeName()));
            state.getStatus().set(ECSNodeState.Status.NOT_LAUNCHED);
            return false;
        }

        logger.info(String.format("Connected to node %s",
                node.getNodeName()));

        state.getStatus().set(ECSNodeState.Status.CONNECTED);

        Metadata currentMetadata = computeMetadata();

        ECSNode successorNode = currentMetadata.getServer(node.getPosition());

        // Perform data transfer

        String rangeStart = null;
        String rangeEnd = null;

        if (successorNode != null) {
            try {
                logger.info("Transferring data");

                // Set write lock

                sendCommandToNode(new KVMessageImpl(null, null,
                        KVMessage.StatusType.ECS_LOCK_WRITE), successorNode);

                ECSNode predecessor = currentMetadata
                        .getPredecessor(successorNode);

                rangeStart = predecessor.getPosition();
                rangeEnd = node.getPosition();

                sendCommandToNode(new KVMessageImpl(null, null,
                                null, KVMessage.StatusType.ECS_COPY_DATA,
                                new MoveDataArgs(rangeStart, rangeEnd,
                                        node.getNodeHost(), node.getNodePort())),
                        successorNode, MOVE_DATA_COMMAND_TIMEOUT_SECONDS);

            } catch (NodeCommandException e) {
                logger.error("Unable to transfer data. Shutting down new node");
                try {
                    sendCommandToNode(new KVMessageImpl(null, null,
                                    KVMessage.StatusType.ECS_UNLOCK_WRITE),
                            successorNode);
                } catch (NodeCommandException nce) {
                    logger.error("Unable to release write lock");
                }
                try {
                    sendCommandToNode(new KVMessageImpl(null, null,
                                    KVMessage.StatusType.ECS_SHUTDOWN), node,
                            SHUTDOWN_COMMAND_TIMEOUT_SECONDS);
                } catch (NodeCommandException nce) {
                    logger.error("Unable to shut down new node");
                }
                state.getStatus().set(ECSNodeState.Status.NOT_LAUNCHED);
                return false;
            }
        }

        // Update metadata on all nodes

        logger.info("Updating metadata of all nodes");

        state.getStatus().set(ECSNodeState.Status.ACTIVATED);

        // This metadata will include the newly activated server
        Metadata newMetadata = computeMetadata();

        try {
            updateActiveNodesMetadata(newMetadata);
        } catch (NodeCommandException e) {
            logger.error("Unable to update metadata on all nodes");
            return false;
        }

        // Release write lock and delete transferred data

        if (successorNode != null) {
            try {
                sendCommandToNode(new KVMessageImpl(null, null,
                        KVMessage.StatusType.ECS_UNLOCK_WRITE), successorNode);
            } catch (NodeCommandException e) {
                logger.error("Unable to release write lock");
                return false;
            }

            try {
                sendCommandToNode(new KVMessageImpl(null, null,
                                null, KVMessage.StatusType.ECS_DELETE_DATA,
                                new MoveDataArgs(rangeStart, rangeEnd, null, 0)),
                        successorNode, MOVE_DATA_COMMAND_TIMEOUT_SECONDS);
            } catch (NodeCommandException e) {
                logger.error("Unable to delete transferred data");
                return false;
            }
        }

        logger.info(String.format("Successfully added node %s",
                node.getNodeName()));

        return true;
    }

    public boolean removeNode(String nodeName) {
        ECSNodeState state = getNodeState(nodeName);
        if (state == null) {
            logger.error("Node not found: " + nodeName);
            return false;
        }

        if (state.getStatus().get() != ECSNodeState.Status.ACTIVATED) {
            logger.error("Node not active: " + nodeName);
            return false;
        }

        ECSNode node = state.getNode();

        // Remove from list of active servers
        state.getStatus().set(ECSNodeState.Status.CONNECTED);

        Metadata newMetadata = computeMetadata();

        ECSNode successorNode = newMetadata.getServer(node.getPosition());

        // Perform data transfer

        if (successorNode != null) {
            try {
                logger.info("Transferring data");

                // Set write lock

                sendCommandToNode(new KVMessageImpl(null, null,
                        KVMessage.StatusType.ECS_LOCK_WRITE), node);

                ECSNode predecessor = newMetadata.getPredecessor(successorNode);

                String rangeStart = predecessor.getPosition();
                String rangeEnd = node.getPosition();

                sendCommandToNode(new KVMessageImpl(null, null, null,
                                KVMessage.StatusType.ECS_COPY_DATA,
                                new MoveDataArgs(rangeStart, rangeEnd,
                                        successorNode.getNodeHost(),
                                        successorNode.getNodePort())),
                        node, MOVE_DATA_COMMAND_TIMEOUT_SECONDS);

            } catch (NodeCommandException e) {
                logger.error("Unable to transfer data. Aborting removal.");
                try {
                    sendCommandToNode(new KVMessageImpl(null, null,
                            KVMessage.StatusType.ECS_UNLOCK_WRITE), node);
                } catch (NodeCommandException nce) {
                    logger.error("Unable to release write lock");
                }
                state.getStatus().set(ECSNodeState.Status.ACTIVATED);
                return false;
            }
        } else {
            logger.warn("Warning: The only active server is being removed.");
        }

        // Shutdown the server to be removed

        boolean success = true;

        logger.info("Shutting down removed node");

        try {
            sendCommandToNode(new KVMessageImpl(null, null,
                            KVMessage.StatusType.ECS_SHUTDOWN), node,
                    SHUTDOWN_COMMAND_TIMEOUT_SECONDS);
        } catch (NodeCommandException e) {
            logger.error("Unable to shut down removed node");
            success = false;
        }

        state.getStatus().set(ECSNodeState.Status.NOT_LAUNCHED);

        // Update metadata on all nodes

        logger.info("Updating metadata of all nodes");

        try {
            updateActiveNodesMetadata(newMetadata);
        } catch (NodeCommandException e) {
            logger.error("Unable to update metadata on all nodes");
            success = false;
        }

        if (success) {
            logger.info(String.format("Successfully removed node %s",
                    node.getNodeName()));
        }

        return success;
    }

    public void addNodes(int count, String cacheStrategy,
                         int cacheSize) {
        List<ECSNode> availableToAdd = getNodesWithStatus(
                ECSNodeState.Status.NOT_LAUNCHED);

        if (availableToAdd.size() < count) {
            String msg = String
                    .format("Not enough available nodes to add. Number of available node: %s",
                            availableToAdd.size());
            logger.error(msg);
            throw new IllegalStateException(msg);
        }

        for (int i = 0; i < count; i++) {
            boolean success = addNode(cacheStrategy, cacheSize);
        }
    }

    public void start() throws NodeCommandException {
        List<ECSNode> nodes = getNodesWithStatus(ECSNodeState.Status.ACTIVATED);
        sendCommandToNodes(new KVMessageImpl(null, null, null,
                KVMessage.StatusType.ECS_START_SERVING), nodes);
    }

    public void stop() throws NodeCommandException {
        List<ECSNode> nodes = getNodesWithStatus(ECSNodeState.Status.ACTIVATED);
        sendCommandToNodes(new KVMessageImpl(null, null, null,
                KVMessage.StatusType.ECS_STOP_SERVING), nodes);
    }

    /**
     * NOTE: Do not use ECSController after calling this method.
     */
    public void shutdownAllNodes() {
        List<ECSNode> nodes = getNodesWithStatus(ECSNodeState.Status.LAUNCHED,
                ECSNodeState.Status.CONNECTED, ECSNodeState.Status.ACTIVATED);
        try {
            sendCommandToNodes(new KVMessageImpl(null, null, null,
                            KVMessage.StatusType.ECS_SHUTDOWN), nodes,
                    SHUTDOWN_COMMAND_TIMEOUT_SECONDS);
        } catch (NodeCommandException e) {
            logger.error("Unable to shutdown all servers");
        }
        for (ECSNode node : nodes) {
            getNodeState(node).getStatus()
                    .set(ECSNodeState.Status.NOT_LAUNCHED);
        }
    }

    public void printServerStatuses() {
        for (ECSNode node : nodes) {
            System.out.println("- " + node.toString());
            System.out.println("  " + getNodeState(node).toString());
        }
    }

    private void updateActiveNodesMetadata(Metadata metadata) throws
            NodeCommandException {
        List<ECSNode> nodes = getNodesWithStatus(ECSNodeState.Status.ACTIVATED);
        sendCommandToNodes(new KVMessageImpl(null, null, metadata,
                KVMessage.StatusType.ECS_UPDATE_METADATA), nodes);
    }

    /**
     * @return a list of nodes which has one of the given statuses
     */
    private List<ECSNode> getNodesWithStatus(ECSNodeState.Status... statuses) {
        List<ECSNode> result = new ArrayList<>();
        Set<ECSNodeState.Status> statusSet = new HashSet<>(
                Arrays.asList(statuses));
        for (ECSNode node : nodes) {
            if (statusSet.contains(getNodeState(node).getStatus().get())) {
                result.add(node);
            }
        }
        return result;
    }

    private void sendCommandToNode(KVMessage message, ECSNode node) throws
            NodeCommandException {
        sendCommandToNode(message, node, DEFAULT_COMMAND_TIMEOUT_SECONDS);
    }

    private void sendCommandToNodes(KVMessage message,
                                    List<ECSNode> nodes) throws
            NodeCommandException {
        sendCommandToNodes(message, nodes, DEFAULT_COMMAND_TIMEOUT_SECONDS);
    }

    /**
     * Send the given message to the given node, and wait for response.
     *
     * @return whether the command succeeded.
     */
    private void sendCommandToNode(KVMessage message,
                                   ECSNode node,
                                   int timeoutSeconds) throws
            NodeCommandException {
        sendCommandToNodes(message, Collections.singletonList(node),
                timeoutSeconds);
    }

    /**
     * Send the given message to the given list of nodes, and wait for
     * responses.
     *
     * @return a list of boolean (corresponding to the same order as the list of
     * nodes) of whether each request succeeded.
     */
    private void sendCommandToNodes(KVMessage message,
                                    List<ECSNode> nodes,
                                    int timeoutSeconds) throws
            NodeCommandException {
        int[] requestIDs = new int[nodes.size()];
        for (int i = 0; i < nodes.size(); ++i) {
            ECSNode node = nodes.get(i);
            ECSNodeState state = getNodeState(node);
            ServerConnection connection = state.getConnection();
            requestIDs[i] = -1;
            if (connection.isConnectionValid()) {
                try {
                    requestIDs[i] = connection
                            .sendRequest(message);
                } catch (Exception e) {
                    // TODO Server stopped unexpectedly. Unhandled for now
                }
            } else {
                // TODO Server stopped unexpectedly. Unhandled for now
            }
        }

        int successCount = 0;

        boolean[] successes = new boolean[nodes.size()];

        for (int i = 0; i < nodes.size(); ++i) {
            ECSNode node = nodes.get(i);
            ECSNodeState state = getNodeState(node);
            ServerConnection connection = state.getConnection();
            successes[i] = false;
            if (connection.isConnectionValid()) {
                if (requestIDs[i] != -1) {
                    try {
                        KVMessage resMessage = connection
                                .receiveMessage(requestIDs[i], timeoutSeconds);
                        if (resMessage
                                .getStatus() == KVMessage.StatusType.ECS_SUCCESS) {
                            // Success
                            successes[i] = true;
                            successCount++;
                        } else {
                            logger.error(String.format(
                                    "Node %s responded with failure: " +
                                            resMessage.getValue(),
                                    node.getNodeName()));
                        }
                    } catch (Exception e) {
                        // TODO Server stopped unexpectedly. Unhandled for now
                        logger.error(String.format(
                                "Unable to receive message from %s",
                                node.getNodeName()));
                    }
                } else {
                    logger.error(String.format(
                            "Unable to send message to %s",
                            node.getNodeName()));
                }
            } else {
                // TODO Server stopped unexpectedly. Unhandled for now
                logger.error(String.format(
                        "Connection of %s not valid",
                        node.getNodeName()));
            }
        }

        logger.info(String.format("%d/%d requests succeeded.", successCount,
                nodes.size()));

        if (!allSuccessful(successes)) {
            throw new NodeCommandException(
                    "Unable to successfully send command to all given nodes",
                    successes);
        }
    }

    public static ArrayList<ECSNode> readConfig(String fileName) {
        ArrayList<ECSNode> servers = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] tokens = line.split(" ");
                if (tokens.length == 3) {
                    ECSNode server = new ECSNode(tokens[0], tokens[1],
                            Integer.parseInt(tokens[2]));
                    servers.add(server);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.printf("%d servers found.%n", servers.size());
        return servers;
    }

    @Override
    public void handleZooKeeperEvent(WatchedEvent event) {
        if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
            try {
                onZooKeeperChildrenChanged(getZooKeeperChildren(true));
            } catch (Exception e) {
                logger.error("Failed to watch ZooKeeper node", e);
            }
        }
    }

    private void onZooKeeperChildrenChanged(List<String> children) {
        logger.info("ZooKeeper children changed.");

        for (String nodeName : children) {
            // TODO remove me
            System.out.println("========== name");
            System.out.println(nodeName);
            String[] components = nodeName.split("/");
            nodeName = components[components.length - 1];
            ECSNodeState state = getNodeState(nodeName);

            AtomicReference<ECSNodeState.Status> status = state.getStatus();

            // These parts may be susceptible to race condition;
            // needs future improvement

            if (status.get() == ECSNodeState.Status.LAUNCHING) {
                state.getStatus()
                        .compareAndSet(ECSNodeState.Status.LAUNCHING,
                                ECSNodeState.Status.LAUNCHED);
                logger.info(
                        String.format("Detected launched node %s",
                                nodeName));
            }
        }
    }

    private List<String> getZooKeeperChildren(boolean watch) throws Exception {
        return zooKeeperService.getChildren(ZOO_KEEPER_ROOT, watch);
    }

    private ECSNodeState getNodeState(String nodeName) {
        return nodeStates.get(nodeName);
    }

    private ECSNodeState getNodeState(ECSNode node) {
        return nodeStates.get(node.getNodeName());
    }

    private Metadata computeMetadata() {
        return new Metadata(getNodesWithStatus(ECSNodeState.Status.ACTIVATED));
    }

    private boolean allSuccessful(boolean[] successes) {
        for (boolean success : successes) {
            if (!success) return false;
        }
        return true;
    }
}
