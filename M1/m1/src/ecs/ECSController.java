package ecs;

import org.apache.log4j.Logger;
import shared.ECSNode;
import shared.messages.IECSNode;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ThreadLocalRandom;

public class ECSController {

    public static final String ZOO_KEEPER_ROOT = "kvECS";

    private static Logger logger = Logger.getRootLogger();

    private ArrayList<ECSNode> servers;
    private ArrayList<ECSNode> availableToAdd;

    private String zooKeeperUrl;

    public ECSController(String configPath, String zooKeeperUrl) throws
            IOException {
        servers = readConfig(configPath);
        availableToAdd = new ArrayList<>(servers);
        this.zooKeeperUrl = zooKeeperUrl;
    }

    public IECSNode addNode(String cacheStrategy, int cacheSize) throws
            IOException {
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
                .format("invoke_server.sh %s %s %s %d %s %s",
                        node.getNodeHost(),
                        node.getNodePort(), cacheStrategy, cacheSize,
                        zooKeeperUrl, node.getNodeName());

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

        availableToAdd.remove(randomNum);
        return servers.get(randomNum);
    }

    public Collection<IECSNode> addNodes(int count, String cacheStrategy,
                                         int cacheSize) throws IOException {
        if (availableToAdd.size() < count) {
            String msg = String
                    .format("Not enough available nodes to add. Number of available node: %s",
                            availableToAdd.size());
            logger.error(msg);
            throw new IllegalStateException(msg);
        }

        ArrayList<IECSNode> addedNodes = new ArrayList();
        for (int i = 0; i < count; i++) {
            try {
                IECSNode node = addNode(cacheStrategy, cacheSize);
                addedNodes.add(node);
            } catch (Exception e) {
                logger.error(e);
            }
        }

        return addedNodes;
    }

    public static ArrayList<ECSNode> readConfig(String fileName) throws
            IOException {
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
