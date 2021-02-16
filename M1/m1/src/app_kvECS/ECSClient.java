package app_kvECS;

import ecs.ECSController;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import shared.messages.IECSNode;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.Map;

public class ECSClient implements IECSClient {

    private static Logger logger = Logger.getRootLogger();
    private static final String PROMPT = "ECS Client> ";
    private final InputStream input;
    private BufferedReader stdin;
    private boolean stop = false;

    private final ECSController controller;

    public ECSClient(InputStream inputStream, String configPath) throws
            IOException {
        this.input = inputStream;
        this.controller = new ECSController(configPath);
    }

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
        try {
            return controller.addNode(cacheStrategy, cacheSize);
        } catch (Exception e) {
            printError("Unable to add node: " + e.getMessage());
            return null;
        }
    }

    @Override
    public Collection<IECSNode> addNodes(int count, String cacheStrategy,
                                         int cacheSize) {
        try {
            return controller.addNodes(count, cacheStrategy, cacheSize);
        } catch (Exception e) {
            printError("Unable to add nodes: " + e.getMessage());
            return null;
        }
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

    public boolean connectionValid() {
        return !stop;
    }

    private void handleCommand(String cmdLine) throws Exception {
        String[] tokens = cmdLine.trim().split("\\s+");

        if (tokens[0].equals("shutdown")) {
            if (tokens.length == 1) {
                stop = true;
                shutdown();
                System.out.println("Application exit!");
            } else {
                printError("Invalid number of parameters!");
            }

        } else if (tokens[0].equals("start")) {
            if (tokens.length == 1) {
                start();
            } else {
                printError("Invalid number of parameters!");
            }

        } else if (tokens[0].equals("stop")) {
            if (tokens.length == 1) {
                stop();
            } else {
                printError("Invalid number of parameters!");
            }

        } else if (tokens[0].equals("addnodes")) {
            if (tokens.length == 2) {
                int nodesNum = Integer.parseInt(tokens[1]);
                String cacheStrategy = tokens[2];
                int cacheSize = Integer.parseInt(tokens[3]);
                addNodes(nodesNum, cacheStrategy, cacheSize);
            } else {
                printError("Invalid number of parameters!");
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
            if (tokens.length == 3) {
                String cacheStrategy = tokens[1];
                int cacheSize = Integer.parseInt(tokens[2]);
                addNode(cacheStrategy, cacheSize);
                System.out.println("Add node");
            } else {
                printError("Invalid number of parameters!");
            }

        } else if (tokens[0].equals("removenode")) {
            if (tokens.length == 2) {
                //To do
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
        sb.append("addnode");
        sb.append("\t add a single node to the system\n");
        sb.append("addnodes <count>");
        sb.append("\t add a given number of nodes to the system\n");
        sb.append("start");
        sb.append("\t signal all servers to start serving\n");
        sb.append("stop");
        sb.append("\t signal all servers to stop serving\n");
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
                this.handleCommand(cmdLine);
            } catch (IOException e) {
                stop = true;
                logger.error("Failed to read from command line", e);
                printError(
                        "ECS Client does not respond - Application terminated ");
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
            new LogSetup("logs/ecsclient.log", Level.OFF);
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        }
        String configPath = args[0];
        ECSClient ecsApp = new ECSClient(System.in, configPath);
        ecsApp.run();
    }
}
