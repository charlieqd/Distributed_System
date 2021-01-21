package app_kvClient;

//import client.Client;

import client.Client;
import client.ClientSocketListener;
import client.KVCommInterface;
import client.KVStore;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import shared.messages.KVMessage;
import shared.messages.KVMessageImpl;
import shared.messages.TextMessage;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;

public class KVClient implements IKVClient, ClientSocketListener {

    private static Logger logger = Logger.getRootLogger();
    private static final String PROMPT = "419Client> ";
    private BufferedReader stdin;
    private Client client = null; //del
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
//                    connect(serverAddress, serverPort);
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
                    String msg = tokenToString(tokens);
                    putData(msg);
                } else {
                    printError("Not connected!");
                }
            } else {
                printError("Invalid number of parameters!");
            }

        } else if (tokens[0].equals("get")) {
            if (tokens.length == 2) {
                if (kvclient != null && kvclient.isRunning()) {
                    String msg = tokenToString(tokens);
                    getData(msg);
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

    private void putData(String msg) throws Exception {
        try {
            KVMessage kvMsg = new KVMessageImpl(msg);
            KVMessage response = kvclient.put(kvMsg.getKey(), kvMsg.getValue());
            KVMessage.StatusType responseStatus = response.getStatus();
            if (responseStatus == KVMessage.StatusType.PUT_ERROR) {
                printError("Put Error!");
            } else if (responseStatus == KVMessage.StatusType.PUT_SUCCESS) {
                System.out.println("Put Data Success!");
            } else if (responseStatus == KVMessage.StatusType.PUT_UPDATE) {
                System.out.println("Put Update Success!");
            } else {
                printError("Unexpected Response Type From Put Request!");
            }
        } catch (IOException e) {
            printError("Unable to send put request!");
            newDisconnection();
        }

    }

    private void getData(String msg) throws Exception {
        try {
            KVMessage kvMsg = new KVMessageImpl(msg);
            KVMessage response = kvclient.get(kvMsg.getKey());
            KVMessage.StatusType responseStatus = response.getStatus();
            if (responseStatus == KVMessage.StatusType.GET_ERROR) {
                printError("Get Error!");
            } else if (responseStatus == KVMessage.StatusType.GET_SUCCESS) {
                System.out.println("Get Data Success!");
            } else {
                printError("Unexpected Response Type From GET Request!");
            }
        } catch (IOException e) {
            printError("Unable to send put request!");
            newDisconnection();
        }

    }

    private void connect(String address, int port)
            throws UnknownHostException, IOException {
        client = new Client(address, port);
        client.addListener(this);
        client.start();
    }

    @Override
    public void newConnection(String address, int port) throws Exception {
        kvclient = new KVStore(address, port);
        kvclient.addListener(this);
        kvclient.start();
    }

    private void disconnect() {
        if (client != null) {
            client.closeConnection();
            client = null;
        }
    }

    private void newDisconnection() {
        if (kvclient != null) {
            kvclient.closeConnection();
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
        sb.append("\t\t get the value of the key from the server \n");
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
    public void handleNewMessage(TextMessage msg) {
        if (!stop) {
            System.out.println(msg.getMsg());
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
        return null;
    }

    private void printError(String error) {
        System.out.println(PROMPT + "Error! " + error);
    }

    /**
     * Main entry point for the echo server application.
     *
     * @param args contains the port number at args[0].
     */
    public static void main(String[] args) {
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
