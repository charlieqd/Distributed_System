package app_kvECS;

import ecs.ECSController;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import shared.ECSNode;
import shared.IProtocol;
import shared.ISerializer;
import shared.Protocol;
import shared.messages.KVMessage;
import shared.messages.KVMessageSerializer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

public class ECSClient {

    private static Logger logger = Logger.getRootLogger();
    private static final String PROMPT = "ECS Client> ";
    
    private final InputStream input;
    private BufferedReader stdin;
    private boolean stop = false;

    private final ECSController controller;

    private static final String DEFAULT_ZOOKEEPER_URL = "127.0.0.1:2181";

    public ECSClient(IProtocol protocol,
                     ISerializer<KVMessage> serializer,
                     InputStream inputStream,
                     String configPath,
                     ZooKeeperService zooKeeperService) throws
            Exception {
        this.input = inputStream;
        this.controller = new ECSController(protocol, serializer, configPath,
                zooKeeperService);
    }

    public boolean connectionValid() {
        return !stop;
    }

    private void handleCommand(String cmdLine) throws Exception {
        String[] tokens = cmdLine.trim().split("\\s+");

        if (tokens[0].equals("shutdown")) {
            if (tokens.length == 1) {
                stop = true;
                System.out.println("Shutting down all active nodes...");
                controller.shutdownAllNodes();
            } else {
                printError("Invalid number of parameters!");
            }

        } else if (tokens[0].equals("start")) {
            if (tokens.length == 1) {
                System.out.println("Starting all active nodes...");
                controller.start();
            } else {
                printError("Invalid number of parameters!");
            }

        } else if (tokens[0].equals("stop")) {
            if (tokens.length == 1) {
                System.out.println("Stopping all active nodes...");
                controller.stop();
            } else {
                printError("Invalid number of parameters!");
            }

        } else if (tokens[0].equals("list")) {
            if (tokens.length == 1) {
                controller.printServerStatuses();
            } else {
                printError("Invalid number of parameters!");
            }

        } else if (tokens[0].equals("addnodes")) {
            List<ECSNode> nodes = null;

            if (tokens.length == 2) {
                System.out.println("Adding nodes...");
                try {
                    int nodesNum = Integer.parseInt(tokens[1]);
                    nodes = controller
                            .addNodes(nodesNum,
                                    ECSController.DEFAULT_CACHE_STRATEGY,
                                    ECSController.DEFAULT_CACHE_SIZE);
                } catch (NumberFormatException e) {
                    printError("Invalid integer");
                }
            } else if (tokens.length == 4) {
                System.out.println("Adding nodes...");
                try {
                    int nodesNum = Integer.parseInt(tokens[1]);
                    String cacheStrategy = tokens[2];
                    int cacheSize = Integer.parseInt(tokens[3]);
                    nodes = controller
                            .addNodes(nodesNum, cacheStrategy, cacheSize);
                } catch (NumberFormatException e) {
                    printError("Invalid integer");
                }
            } else {
                printError("Invalid number of parameters!");
            }

            if (nodes != null) {
                System.out.println("Nodes added:");
                for (ECSNode node : nodes) {
                    System.out
                            .printf("- name: %s, address: %s, port: %d\n",
                                    node.getNodeName(), node.getNodeHost(),
                                    node.getNodePort());
                }
            }

        } else if (tokens[0].equals("logLevel")) {
            if (tokens.length == 2) {
                String level = LogSetup.setLogLevel(tokens[1]);
                if (level.equals(LogSetup.UNKNOWN_LEVEL)) {
                    printError("No valid log level!");
                    printPossibleLogLevels();
                } else {
                    System.out.println("Log level changed to level " + level);
                }
            } else {
                printError("Invalid number of parameters!");
            }

        } else if (tokens[0].equals("addnode")) {
            ECSNode node = null;
            if (tokens.length == 1) {
                System.out.println("Adding node...");
                node = controller
                        .addNode(ECSController.DEFAULT_CACHE_STRATEGY,
                                ECSController.DEFAULT_CACHE_SIZE);
            } else if (tokens.length == 3) {
                System.out.println("Adding node...");
                try {
                    String cacheStrategy = tokens[1];
                    int cacheSize = Integer.parseInt(tokens[2]);
                    node = controller.addNode(cacheStrategy, cacheSize);
                } catch (NumberFormatException e) {
                    printError("Invalid integer");
                }
            } else {
                printError("Invalid number of parameters!");
            }

            if (node != null) {
                System.out
                        .printf("Node added: name: %s, address: %s, port: %d\n",
                                node.getNodeName(), node.getNodeHost(),
                                node.getNodePort());
            }

        } else if (tokens[0].equals("removenode")) {
            if (tokens.length == 2) {
                System.out.println("Removing node...");
                String nodeName = tokens[1];
                controller.removeNode(nodeName);
            } else {
                printError("Invalid number of parameters!");
            }
        } else if (tokens[0].equals("help")) {
            printHelp();
        } else {
            printError("Unknown command");
            printHelp();
        }
    }

    private void printPossibleLogLevels() {
        System.out.println("Possible log levels are:");
        System.out.println("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF");
    }

    private void printHelp() {
        StringBuilder sb = new StringBuilder();
        sb.append("ECS CLIENT HELP (Usage):\n");
        sb.append("::::::::::::::::::::::::::::::::");
        sb.append("::::::::::::::::::::::::::::::::\n");
        sb.append("addnode [cacheStrategy] [cacheSize]");
        sb.append("\t add a single node to the system\n");
        sb.append("addnodes <count> [cacheStrategy] [cacheSize]");
        sb.append("\t add a given number of nodes to the system\n");
        sb.append("start");
        sb.append("\t signal all servers to start serving\n");
        sb.append("stop");
        sb.append("\t signal all servers to stop serving\n");
        sb.append("list");
        sb.append("\t print server statuses for debug\n");
        sb.append("removenode <index>");
        sb.append("\t remove a server from the system\n");
        sb.append("shutdown");
        sb.append("\t stop all server instances and exit\n");
        sb.append("logLevel");
        sb.append("\t changes the logLevel \n");
        sb.append("\t\t\t ");
        sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");
        System.out.println(sb.toString());
    }

    public void run() throws Exception {
        stdin = new BufferedReader(new InputStreamReader(this.input));
        while (!stop) {
            System.out.print(PROMPT);
            try {
                String cmdLine = stdin.readLine();
                try {
                    this.handleCommand(cmdLine);
                } catch (Exception e) {
                    printError("Internal server error while handling command");
                    logger.error(e);
                }
            } catch (IOException e) {
                stop = true;
                logger.error("Failed to read from command line", e);
                printError(
                        "ECS Client does not respond - Application terminated ");
            } catch (Exception e) {
                stop = true;
                logger.error("Failed to read from command line", e);
            }

            System.out.println("");
        }
    }

    private void printError(String error) {
        System.out.println("Error: " + error);
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.out.println("usage: <config-file-path>");
            System.exit(1);
        }

        try {
            new LogSetup("logs/ecsclient.log", Level.WARN);
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        }
        String configPath = args[0];

        IProtocol protocol = new Protocol();
        ISerializer<KVMessage> messageSerializer = new KVMessageSerializer();

        ZooKeeperService zooKeeperService = new ZooKeeperService(
                DEFAULT_ZOOKEEPER_URL);

        ECSClient ecsApp = new ECSClient(protocol, messageSerializer, System.in,
                configPath, zooKeeperService);
        ecsApp.run();
    }
}
