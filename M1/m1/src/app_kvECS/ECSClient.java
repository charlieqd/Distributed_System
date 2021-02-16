package app_kvECS;

import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import shared.ECSNode;
import shared.messages.IECSNode;

import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class ECSClient implements IECSClient {

    private ArrayList<ECSNode> servers;
    private ArrayList<ECSNode> serversAdded;
    private static Logger logger = Logger.getRootLogger();
    private static final String PROMPT = "ECS Client> ";
    private final InputStream input;
    private BufferedReader stdin;
    private boolean stop = false;

    ArrayList<ECSNode> availableToAdd;

    public ECSClient(InputStream inputStream, ArrayList<ECSNode> servers) {
        this.input = inputStream;
        this.servers = servers;
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
        // TODO
        if (availableToAdd.isEmpty()) {
            String msg = "No node is available to add.";
            logger.error(msg);
            printError(msg);
        }
        Process proc;
        String script = String
                .format("invoke_server.sh %s %s", servers.get(0).getNodeHost(),
                        servers.get(0).getNodePort());

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

    public static ArrayList<ECSNode> readConfig(String fileName) throws IOException {
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

            } else {
                printError("Invalid number of parameters!");
            }
        } else if (tokens[0].equals("addNodes")) {
            if (tokens.length == 1) {
                int nodesNum = Integer.parseInt(tokens[1]);

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
            if (tokens.length == 1) {
                System.out.println("Add node");
            } else {
                printError("Invalid number of parameters!");
            }

        } else if (tokens[0].equals("removeNode")) {
            if (tokens.length == 2) {
                if (connectionValid()) {
                    String key = tokens[1];
                } else {
                    printError("Not connected or connection stopped!");
                }
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
        sb.append("\t establishes a connection to a server\n");
        sb.append("Add Nodes");
        sb.append("\t\t get the value from the server \n");
        sb.append("add node <key> [<value>]");
        sb.append(
                "\t insert or update a tuple. if value is not given, delete the tuple. \n");
        sb.append("remove Nodes <key>");
        sb.append("\t\t delete the tuple from the server \n");
        sb.append("shut down");
        sb.append("\t\t disconnects from the server \n");

        sb.append("logLevel");
        sb.append("\t\t changes the logLevel \n");
        sb.append("\t\t\t ");
        sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");

        sb.append("stop");
        sb.append("\t\t\t exits the program");
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
                printError("ECS Client does not respond - Application terminated ");
            }

            System.out.println("");
        }
    }

    private void printError(String error) {
        System.out.println("Error: " + error);
    }

    public static void main(String[] args) throws Exception {
        // TODO
        // read config into a list of serverInfo
        String fileName = args[0];
        ArrayList<ECSNode> servers = readConfig(fileName);
        ECSClient ecsApp = new ECSClient(System.in, servers);
        ecsApp.run();


    }
}
