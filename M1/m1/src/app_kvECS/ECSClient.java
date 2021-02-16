package app_kvECS;

import ecs.IECSNode;
import shared.ServerInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

public class ECSClient implements IECSClient {

    ArrayList<ServerInfo> availableToAdd;

    @Override
    public boolean start() {
        // TODO
        return false;
    }

    @Override
    public boolean stop() {
        // TODO
        return false;
    }

    @Override
    public boolean shutdown() {
        // TODO
        return false;
    }

    @Override
    public IECSNode addNode(String cacheStrategy, int cacheSize) {
        // TODO
        if (availableToAdd.isEmpty()) {
            String msg = "No node is available to add.";
            logger.error(msg);
            printError(msg);
        }
        Process proc;
        String script = String
                .format("invoke_server.sh %s %s", servers.get(0).ip,
                        servers.get(0).port);

        Runtime run = Runtime.getRuntime();
        try {
            proc = run.exec(script);
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("Failed to add nodes", e);
            printError("Failed to add node");
        }
        return null;
    }

    @Override
    public Collection<IECSNode> addNodes(int count, String cacheStrategy,
                                         int cacheSize) {
        // TODO
        if (availableToAdd.size() < count) {
            String msg = String
                    .format("No enough nodes to add. Number of available node: %s",
                            availableToAdd.size());
            logger.error(msg);
            printError(msg);
            return null;
        }

        for (int i = 0; i < count; i++) {
            addNode(cacheStrategy, cacheSize);
        }
        return null;
    }

    @Override
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy,
                                           int cacheSize) {
        // TODO
        return null;
    }

    @Override
    public boolean awaitNodes(int count, int timeout) throws Exception {
        // TODO
        return false;
    }

    @Override
    public boolean removeNodes(Collection<String> nodeNames) {
        // TODO
        return false;
    }

    @Override
    public Map<String, IECSNode> getNodes() {
        // TODO
        return null;
    }

    @Override
    public IECSNode getNodeByKey(String Key) {
        // TODO
        return null;
    }

    public static void main(String[] args) {
        // TODO
    }
}
