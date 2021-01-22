package app_kvClient;

//import client.Client;

import client.ClientSocketListener;
import client.KVCommInterface;
import client.KVStore;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import shared.messages.KVMessage;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;

public class KVClient implements IKVClient, ClientSocketListener {

    private static Logger logger = Logger.getRootLogger();
    private static final String PROMPT = "419Client> ";
    private BufferedReader stdin;
    private KVStore kvclient = null;
    private boolean stop = false;

    private String serverAddress;
    private int serverPort;

    public void run() throws Exception {
        while (!stop) {
            stdin = new BufferedReader(new InputStreamReader(System.in));
            System.out.print(PROMPT);

            try {
                String cmdLine = stdin.readLine();
                this.handleCommand(cmdLine);
            } catch (IOException e) {
                stop = true;
                printError("CLI does not respond - Application terminated ");
            }
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
        String[] tokens = cmdLine.split("\\s+");

        if (tokens[0].equals("quit")) {
            stop = true;
            newDisconnection();
            System.out.println(PROMPT + "Application exit!");

        } else if (tokens[0].equals("connect")) {
            if (tokens.length == 3) {
                try {
                    serverAddress = tokens[1];
                    serverPort = Integer.parseInt(tokens[2]);
                    newConnection(serverAddress, serverPort);
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
            } else {
                printError("Invalid number of parameters!");
            }

        } else if (tokens[0].equals("disconnect")) {
            newDisconnection();

        } else if (tokens[0].equals("logLevel")) {
            if (tokens.length == 2) {
                String level = setLevel(tokens[1]);
                if (level.equals(LogSetup.UNKNOWN_LEVEL)) {
                    printError("No valid log level!");
                    printPossibleLogLevels();
                } else {
                    System.out.println(PROMPT +
                            "Log level changed to level " + level);
                }
            } else {
                printError("Invalid number of parameters!");
            }

        } else if (tokens[0].equals("put")) {
            if (tokens.length == 3) {
                if (kvclient != null && kvclient.isRunning()) {
//                    String msg = tokenToString(tokens);
                    String key = tokens[1];
                    String value = tokens[2];
                    putData(key, value);
                } else {
                    printError("Not connected!");
                }
            } else {
                printError("Invalid number of parameters!");
            }

        } else if (tokens[0].equals("get")) {
            if (tokens.length == 2) {
                if (kvclient != null && kvclient.isRunning()) {
//                    String msg = tokenToString(tokens);
                    String key = tokens[1];
                    getData(key);
                } else {
                    printError("Not connected!");
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
        try {
            KVMessage msgBack = kvclient.put(key, value);
            KVMessage.StatusType status = msgBack.getStatus();
            if (status == KVMessage.StatusType.PUT_ERROR) {
                printError("Put Error!");
            } else if (status == KVMessage.StatusType.PUT_SUCCESS) {
                System.out.println("Put Data Success!");
            } else if (status == KVMessage.StatusType.PUT_UPDATE) {
                System.out.println("Put Update Success!");
            } else {
                printError("Unexpected Response Type From Put Request!");
            }
        } catch (IOException e) {
            printError("Unable to send put request!");
            newDisconnection();
        }
    }

    private void getData(String key) throws Exception {
        try {
            KVMessage msgBack = kvclient.get(key);
            KVMessage.StatusType status = msgBack.getStatus();
            String value = msgBack.getValue();
            if (status == KVMessage.StatusType.GET_ERROR) {
                printError("Get Error!");
            } else if (status == KVMessage.StatusType.GET_SUCCESS) {
                System.out.println("Get Data Success!");
                System.out.println(value);
            } else {
                printError("Unexpected Response Type From GET Request!");
            }
        } catch (IOException e) {
            printError("Unable to send GET request!");
            newDisconnection();
        }

    }

    @Override
    public void newConnection(String address, int port) throws Exception {
        kvclient = new KVStore(address, port);
        kvclient.connect();
        kvclient.addListener(this);
//        kvclient.start();
    }

    private void newDisconnection() {
        if (kvclient != null) {
            kvclient.disconnect();
            kvclient = null;
        }
    }

    private void printHelp() {
        StringBuilder sb = new StringBuilder();
        sb.append(PROMPT).append("ECHO CLIENT HELP (Usage):\n");
        sb.append(PROMPT);
        sb.append("::::::::::::::::::::::::::::::::");
        sb.append("::::::::::::::::::::::::::::::::\n");
        sb.append(PROMPT).append("connect <host> <port>");
        sb.append("\t establishes a connection to a server\n");
        sb.append(PROMPT).append("put <key> <value>");
        sb.append("\t\t insert or update a new tuple to the server \n");
        sb.append(PROMPT).append("get <key>");
        sb.append("\t\t\t get the value of the key from the server \n");
        sb.append(PROMPT).append("disconnect");
        sb.append("\t\t\t disconnects from the server \n");

        sb.append(PROMPT).append("logLevel");
        sb.append("\t\t\t changes the logLevel \n");
        sb.append(PROMPT).append("\t\t\t\t ");
        sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");

        sb.append(PROMPT).append("quit ");
        sb.append("\t\t\t exits the program");
        System.out.println(sb.toString());
    }

    private void printPossibleLogLevels() {
        System.out.println(PROMPT
                + "Possible log levels are:");
        System.out.println(PROMPT
                + "ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF");
    }

    private String setLevel(String levelString) {

        if (levelString.equals(Level.ALL.toString())) {
            logger.setLevel(Level.ALL);
            return Level.ALL.toString();
        } else if (levelString.equals(Level.DEBUG.toString())) {
            logger.setLevel(Level.DEBUG);
            return Level.DEBUG.toString();
        } else if (levelString.equals(Level.INFO.toString())) {
            logger.setLevel(Level.INFO);
            return Level.INFO.toString();
        } else if (levelString.equals(Level.WARN.toString())) {
            logger.setLevel(Level.WARN);
            return Level.WARN.toString();
        } else if (levelString.equals(Level.ERROR.toString())) {
            logger.setLevel(Level.ERROR);
            return Level.ERROR.toString();
        } else if (levelString.equals(Level.FATAL.toString())) {
            logger.setLevel(Level.FATAL);
            return Level.FATAL.toString();
        } else if (levelString.equals(Level.OFF.toString())) {
            logger.setLevel(Level.OFF);
            return Level.OFF.toString();
        } else {
            return LogSetup.UNKNOWN_LEVEL;
        }
    }

    @Override
    public void handleNewMessage(KVMessage msg) {
        if (!stop) {
            String status = msg.getStatus().name();
            String key = msg.getKey();
            String value = msg.getValue();
            System.out.println(status + " " + key + " " + value);
            System.out.print(PROMPT);
        }
    }

    @Override
    public void handleStatus(SocketStatus status) {
        if (status == SocketStatus.CONNECTED) {

        } else if (status == SocketStatus.DISCONNECTED) {
            System.out.print(PROMPT);
            System.out.println("Connection terminated: "
                    + serverAddress + " / " + serverPort);

        } else if (status == SocketStatus.CONNECTION_LOST) {
            System.out.println("Connection lost: "
                    + serverAddress + " / " + serverPort);
            System.out.print(PROMPT);
        }

    }

    @Override
    public KVCommInterface getStore() {
        return kvclient;
    }

    private void printError(String error) {
        System.out.println(PROMPT + "Error! " + error);
    }

    /**
     * Main entry point for the echo server application.
     *
     * @param args contains the port number at args[0].
     */
    public static void main(String[] args) throws Exception {
        try {
            new LogSetup("logs/client.log", Level.OFF);
            KVClient app = new KVClient();
            app.run();
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        }
    }

}
