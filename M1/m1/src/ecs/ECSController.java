package ecs;

import client.ServerConnection;
import org.apache.log4j.Logger;
import shared.ECSNode;
import shared.IProtocol;
import shared.ISerializer;
import shared.messages.IECSNode;
import shared.messages.KVMessage;
import shared.messages.KVMessageImpl;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * NOTE: This class it not thread-safe.
 */
public class ECSController {
    // The following is guaranteed for each ServerConnection:
    // - Each ServerConnection will only be accessed by one thread at a time.
    //   This is done by setting each node status to LAUNCHED only after
    //   ServerConnection is connected, and no commands are sent unless node
    //   status is LAUNCHED.
    // - Exactly one sendRequest will be called before each receiveMessage.

    public static final String zooKeeperRoot = "kvECS";

    private static Logger logger = Logger.getRootLogger();

    private ArrayList<ECSNode> servers;
    private Map<ECSNode, ECSNodeState> nodeStates;

    public ECSController(IProtocol protocol,
                         ISerializer<KVMessage> serializer,
                         String configPath) {
        servers = readConfig(configPath);
        nodeStates = new HashMap<>();
        for (ECSNode node : servers) {
            nodeStates.put(node, new ECSNodeState(protocol, serializer, node));
        }
    }

    public IECSNode addNode(String cacheStrategy, int cacheSize) throws
            IOException {
        List<ECSNode> availableToAdd = getNodesWithStatus(
                ECSNodeState.Status.NOT_LAUNCHED);
        if (availableToAdd.isEmpty()) {
            String msg = "No available nodes to add.";
            logger.error(msg);
            throw new IllegalStateException(msg);
        }

        Process proc;
        int randomNum = ThreadLocalRandom.current()
                .nextInt(0, availableToAdd.size());
        ECSNode node = availableToAdd.get(randomNum);
        String script = String
                .format("invoke_server.sh %s %s %s %d", node.getNodeHost(),
                        node.getNodePort(), cacheStrategy, cacheSize);

        Runtime run = Runtime.getRuntime();
        try {
            proc = run.exec(script);
        } catch (IOException e) {
            String msg = String
                    .format("Failed to add node, name: %s, host: %s, port: %s",
                            node.getNodeName(), node.getNodeHost(),
                            node.getNodePort());
            logger.error(msg, e);
            throw e;
        }

        nodeStates.get(node).setStatus(ECSNodeState.Status.LAUNCHING);

        logger.info(String.format("Added node: %s %s:%d", node.getNodeName(),
                node.getNodeHost(), node.getNodePort()));

        return servers.get(randomNum);
    }

    public Collection<IECSNode> addNodes(int count, String cacheStrategy,
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

        ArrayList<IECSNode> addedNodes = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            try {
                IECSNode node = addNode(cacheStrategy, cacheSize);
                addedNodes.add(node);
            } catch (Exception e) {
                // Ignore exception
            }
        }

        return addedNodes;
    }

    public void start() {
        List<ECSNode> nodes = getNodesWithStatus(ECSNodeState.Status.LAUNCHED);
        sendCommandToNodes(new KVMessageImpl(null, null, null,
                KVMessage.StatusType.ECS_START_SERVING), nodes);
    }

    public void stop() {
        List<ECSNode> nodes = getNodesWithStatus(ECSNodeState.Status.LAUNCHED);
        sendCommandToNodes(new KVMessageImpl(null, null, null,
                KVMessage.StatusType.ECS_STOP_SERVING), nodes);
    }

    public void removeNode(int index) {
        throw new Error("Not implemented");
    }

    public void shutdownAllNodes() {
        List<ECSNode> nodes = getNodesWithStatus(ECSNodeState.Status.LAUNCHED);
        sendCommandToNodes(new KVMessageImpl(null, null, null,
                KVMessage.StatusType.ECS_SHUT_DOWN), nodes);
        for (ECSNode node : nodes) {
            nodeStates.get(node).setStatus(ECSNodeState.Status.NOT_LAUNCHED);
        }
    }

    /**
     * @return a list of nodes which has one of the given statuses
     */
    private List<ECSNode> getNodesWithStatus(ECSNodeState.Status... statuses) {
        List<ECSNode> result = new ArrayList<>();
        Set<ECSNodeState.Status> statusSet = new HashSet<>(
                Arrays.asList(statuses));
        for (ECSNode node : servers) {
            if (statusSet.contains(nodeStates.get(node).getStatus())) {
                result.add(node);
            }
        }
        return result;
    }

    /**
     * Send the given message to the given node, and wait for response.
     *
     * @return whether the command succeeded.
     */
    private boolean sendCommandToNode(KVMessage message, ECSNode node) {
        return sendCommandToNodes(message,
                Collections.singletonList(node))[0];
    }

    /**
     * Send the given message to the given list of nodes, and wait for
     * responses.
     *
     * @return a list of boolean (corresponding to the same order as the list of
     * nodes) of whether each request succeeded.
     */
    private boolean[] sendCommandToNodes(KVMessage message,
                                         List<ECSNode> nodes) {
        int[] requestIDs = new int[nodes.size()];
        for (int i = 0; i < nodes.size(); ++i) {
            ECSNode node = nodes.get(i);
            ECSNodeState state = nodeStates.get(node);
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
            ECSNodeState state = nodeStates.get(node);
            ServerConnection connection = state.getConnection();
            successes[i] = false;
            if (connection.isConnectionValid()) {
                if (requestIDs[i] != -1) {
                    try {
                        KVMessage resMessage = connection
                                .receiveMessage(requestIDs[i]);
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

        return successes;
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

}
