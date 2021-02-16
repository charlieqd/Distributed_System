package app_kvECS;

import logger.LogSetup;
import org.apache.log4j.Logger;
import shared.ECSNode;
import shared.messages.IECSNode;

import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class ECSClient implements IECSClient {

    private ArrayList<ECSNode> servers;
    private static Logger logger = Logger.getRootLogger();
    private static final String PROMPT = "ECS Client> ";
    private final InputStream input;
    private BufferedReader stdin;
    private boolean stop = false;

    ArrayList<ECSNode> availableToAdd;

    public ECSClient(InputStream inputStream, ArrayList<ECSNode> servers) {
        this.input = inputStream;
        this.servers = servers;
        ArrayList<ECSNode> availableToAdd = new ArrayList<>();
        for (ECSNode s : servers) {
            availableToAdd.add(s);
        }
        this.availableToAdd = availableToAdd;
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
        if (availableToAdd.isEmpty()) {
            String msg = "No node is available to add.";
            logger.error(msg);
            printError(msg);
            return null;
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
            e.printStackTrace();
            String msg = String
                    .format("Failed to add nodes, name: %s, host: %s, port: %s",
                            node.getNodeName(), node.getNodeHost(),
                            node.getNodePort());
            logger.error(msg, e);
            printError(msg);
            return null;
        }

        availableToAdd.remove(randomNum);
        return servers.get(randomNum);
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

        ArrayList<IECSNode> addedNodes = new ArrayList();
        for (int i = 0; i < count; i++) {
            addedNodes.add(addNode(cacheStrategy, cacheSize));
        }

        return addedNodes;
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

    public static ArrayList<ECSNode> readConfig(String fileName) throws
            IOException {
        ArrayList<ECSNode> servers = new ArrayList<>();
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(fileName));
            String line;
            while ((line = br.readLine()) != null) {
                String[] tokens = line.split(" ");
                ECSNode server = new ECSNode(tokens[1],
                        Integer.parseInt(tokens[2]), "");
                servers.add(server);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                br.close();
            }
            return servers;
        }
    }

    public boolean connectionValid() {
        return !stop;
    }

    private void handleCommand(String cmdLine) throws Exception {
        String[] tokens = cmdLine.trim().split("\\s+");

        if (tokens[0].equals("shut") && tokens[1].equals("down")) {
            stop = true;
            shutdown();
            System.out.println("Application exit!");
        } else if (tokens[0].equals("start")) {
            if (tokens.length == 1) {
                start();
            } else {
                printError("Invalid number of parameters!");
            }
         }else if (tokens[0].equals("stop")) {
            if (tokens.length == 1) {
                stop();
            } else {
                printError("Invalid number of parameters!");
            }
        } else if (tokens[0].equals("addNodes")) {
            if (tokens.length == 1) {
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

        } else if (tokens[0].equals("addNode")) {
            if (tokens.length == 3) {
                String cacheStrategy = tokens[1];
                int cacheSize = Integer.parseInt(tokens[2]);
                addNode(cacheStrategy, cacheSize);
                System.out.println("Add node");
            } else {
                printError("Invalid number of parameters!");
            }

        } else if (tokens[0].equals("removeNode")) {
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
        sb.append("start");
        sb.append("\t Starts the storage service\n");
        sb.append("addNodes(int numberOfNodes)");
        sb.append("\t\t Randomly choose <numberOfNodes> servers from the available machines and start the Servers. \n");
        sb.append("add node <key> [<value>]");
        sb.append(
                "\t Create a new KVServer and add it to the storage service. \n");
        sb.append("removeNode(<index of server>)");
        sb.append("\t\t Remove a server from the storage service \n");
        sb.append("shut down");
        sb.append("\t\t Stops all server instances and exits the remote processes. \n");

        sb.append("logLevel");
        sb.append("\t\t changes the logLevel \n");
        sb.append("\t\t\t ");
        sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");

        sb.append("stop");
        sb.append("\t\t\t Stops the service");
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
        try{
            new LogSetup("logs/client.log", Level.OFF);
            String fileName = args[0];
            ArrayList<ECSNode> servers = readConfig(fileName);
            ECSClient ecsApp = new ECSClient(System.in, servers);
            ecsApp.run();
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        }



    }
}
