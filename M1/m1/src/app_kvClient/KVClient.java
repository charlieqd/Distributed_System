package app_kvClient;

//import client.Client;

import client.KVCommInterface;
import client.KVStore;
import client.KVStoreListener;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import shared.Util;
import shared.messages.KVMessage;
import shared.messages.KVMessageImpl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.UnknownHostException;

public class KVClient implements IKVClient, KVStoreListener {

    private static Logger logger = Logger.getRootLogger();
    private static final String PROMPT = "KVClient> ";
    private final InputStream input;
    private BufferedReader stdin;
    private KVStore kvStore = null;
    private boolean stop = false;

    private String serverAddress;
    private int serverPort;

    public KVClient(InputStream inputStream) {
        this.input = inputStream;
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
                printError("CLI does not respond - Application terminated ");
            }

            System.out.println("");
        }
    }

    private String tokenToString(String[] tokens) {
        StringBuilder msg = new StringBuilder();
        for (int i = 0; i < tokens.length; i++) {
            msg.append(tokens[i]);
            if (i != tokens.length - 1) {
                msg.append(" ");
            }
        }
        return msg.toString();
    }

    private void handleCommand(String cmdLine) throws Exception {
        String[] tokens = cmdLine.trim().split("\\s+");

        if (tokens[0].equals("quit")) {
            stop = true;
            disconnect();
            System.out.println("Application exit!");

        } else if (tokens[0].equals("connect")) {
            if (tokens.length == 3) {
                if (connectionValid()) {
                    printError("Already connected!");
                } else {
                    try {
                        serverAddress = tokens[1];
                        serverPort = Integer.parseInt(tokens[2]);
                        disconnect();
                        connect(serverAddress, serverPort);
                    } catch (NumberFormatException nfe) {
                        printError("No valid address. Port must be a number!");
                        logger.info("Unable to parse argument <port>", nfe);
                    } catch (UnknownHostException e) {
                        printError("Unknown Host!");
                        logger.info("Unknown Host!", e);
                    } catch (IOException e) {
                        printError("Could not establish connection!");
                        logger.warn("Could not establish connection!", e);
                    }
                }
            } else {
                printError("Invalid number of parameters!");
            }

        } else if (tokens[0].equals("disconnect")) {
            if (tokens.length == 1) {
                if (kvStore != null) {
                    System.out.println("Disconnecting.");
                    disconnect();
                } else {
                    printError("No connection to close");
                }
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

        } else if (tokens[0].equals("put")) {
            if (tokens.length == 3 || tokens.length == 2) {
                if (connectionValid()) {
                    String key = tokens[1];
                    String value = tokens.length == 3 ? tokens[2] : null;
                    putData(key, value);
                } else {
                    printError("Not connected or connection stopped!");
                }
            } else {
                printError("Invalid number of parameters!");
            }

        } else if (tokens[0].equals("delete")) {
            if (tokens.length == 2) {
                if (connectionValid()) {
                    String key = tokens[1];
                    putData(key, null);
                } else {
                    printError("Not connected or connection stopped!");
                }
            } else {
                printError("Invalid number of parameters!");
            }

        } else if (tokens[0].equals("get")) {
            if (tokens.length == 2) {
                if (connectionValid()) {
                    String key = tokens[1];
                    getData(key);
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

    private void putData(String key, String value) throws Exception {
        if (key != null &&
                key.length() > KVMessageImpl.MAX_KEY_LENGTH) {
            printError(
                    "Key too large; max length " + KVMessageImpl.MAX_KEY_LENGTH);
            return;
        }

        if (value != null &&
                value.length() > KVMessageImpl.MAX_VALUE_LENGTH) {
            printError(
                    "Value too large; max length " + KVMessageImpl.MAX_VALUE_LENGTH);
            return;
        }

        try {
            KVMessage message = kvStore.put(key, value);
            handleMessage(message);
        } catch (IOException e) {
            printError("Unable to send PUT request!");
            logger.warn("Unable to send PUT request", e);
            disconnect();
        }
    }

    private void getData(String key) throws Exception {
        if (key != null &&
                key.length() > KVMessageImpl.MAX_KEY_LENGTH) {
            printError(
                    "Key too large; max length " + KVMessageImpl.MAX_KEY_LENGTH);
            return;
        }

        try {
            KVMessage message = kvStore.get(key);
            handleMessage(message);
        } catch (IOException e) {
            printError("Unable to send GET request!");
            logger.warn("Unable to send GET request", e);
            disconnect();
        }

    }

    public boolean connectionValid() {
        return kvStore != null && kvStore.isConnectionValid();
    }

    @Override
    public void connect(String address, int port) throws Exception {
        if (kvStore == null) {
            kvStore = new KVStore(address, port);
            try {
                kvStore.connect();
                System.out.println("Connection established.");
            } catch (Exception e) {
                kvStore = null;
                logger.warn("Connection failed", e);
                printError("Connection failed: " + Util.getStackTraceString(e));
            }
        }
    }

    private void disconnect() {
        if (kvStore != null) {
            try {
                kvStore.disconnect();
                kvStore = null;
            } catch (Exception e) {
                logger.error("Failed to disconnect", e);
                printError("Failed to disconnect.");
            }
        }
    }

    private void printHelp() {
        StringBuilder sb = new StringBuilder();
        sb.append("KV CLIENT HELP (Usage):\n");
        sb.append("::::::::::::::::::::::::::::::::");
        sb.append("::::::::::::::::::::::::::::::::\n");
        sb.append("connect <host> <port>");
        sb.append("\t establishes a connection to a server\n");
        sb.append("get <key>");
        sb.append("\t\t get the value from the server \n");
        sb.append("put <key> [<value>]");
        sb.append(
                "\t insert or update a tuple. if value is not given, delete the tuple. \n");
        sb.append("delete <key>");
        sb.append("\t\t delete the tuple from the server \n");
        sb.append("disconnect");
        sb.append("\t\t disconnects from the server \n");

        sb.append("logLevel");
        sb.append("\t\t changes the logLevel \n");
        sb.append("\t\t\t ");
        sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");

        sb.append("quit ");
        sb.append("\t\t\t exits the program");
        System.out.println(sb.toString());
    }

    private void printPossibleLogLevels() {
        System.out.println("Possible log levels are:");
        System.out.println("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF");
    }


    public void handleMessage(KVMessage msg) {
        KVMessage.StatusType status = msg.getStatus();
        String key = msg.getKey();
        String value = msg.getValue();
        switch (status) {
            case GET_ERROR:
                System.out.println("Tuple does not exist.");
                break;
            case GET_SUCCESS:
                System.out.println("Value: " + value);
                break;
            case PUT_SUCCESS:
                System.out.println("Tuple inserted.");
                break;
            case PUT_UPDATE:
                System.out.println("Tuple updated.");
                break;
            case PUT_ERROR:
                System.out.println("Error: failed to insert tuple.");
                break;
            case DELETE_SUCCESS:
                System.out.println("Tuple deleted.");
                break;
            case DELETE_ERROR:
                System.out.println("Tuple does not exist.");
                break;
            case SERVER_WRITE_LOCK:
                System.out
                        .println("Failed: server has locked write operations.");
                break;
            case SERVER_STOPPED:
                System.out.println(
                        "Failed: server has stopped or not yet started.");
                break;
            case FAILED:
                System.out.println("Failed: " + value);
                break;
            default:
                printError("Invalid status: " + status.name());
                break;
        }
    }

    public void handleStatusChange(SocketStatus status) {
        if (status == SocketStatus.DISCONNECTED) {
            System.out.println("Connection terminated: "
                    + serverAddress + " / " + serverPort);
            System.out.print(PROMPT);
        }
    }

    @Override
    public KVCommInterface getStore() {
        return kvStore;
    }

    private void printError(String error) {
        System.out.println("Error: " + error);
    }

    /**
     * Main entry point for the client application.
     */
    public static void main(String[] args) throws Exception {
        try {
            new LogSetup("logs/client.log", Level.OFF);
            KVClient app = new KVClient(System.in);
            app.run();
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        }
    }

}
