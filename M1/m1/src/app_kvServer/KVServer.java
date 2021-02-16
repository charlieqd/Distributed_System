package app_kvServer;

import logger.LogSetup;
import org.apache.commons.cli.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import server.*;
import shared.IProtocol;
import shared.ISerializer;
import shared.Metadata;
import shared.Protocol;
import shared.messages.KVMessage;
import shared.messages.KVMessageSerializer;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;

public class KVServer extends Thread implements IKVServer {

    private static Logger logger = Logger.getRootLogger();

    private static final String DEFAULT_CACHE_SIZE = "8192";
    private static final String DEFAULT_CACHE_STRATEGY = "FIFO";
    private static final String DEFAULT_PORT = "8080";
    private static final String DEFAULT_DATA_PATH = "data";
    private static final String DEFAULT_LOG_LEVEL = "ALL";

    private int port;
    private ServerSocket serverSocket;
    private boolean running;
    private final IKVStorage storage;
    private final IProtocol protocol;
    private final ISerializer<KVMessage> messageSerializer;

    /**
     * Start KV Server at given port
     *
     * @param storage           the interface to access persistent storage.
     * @param protocol          the message protocol encoder/decoder.
     * @param messageSerializer serializer for messages.
     * @param port              given port for storage server to operate
     */
    public KVServer(IKVStorage storage,
                    IProtocol protocol,
                    ISerializer<KVMessage> messageSerializer,
                    int port) {
        this.storage = storage;
        this.protocol = protocol;
        this.messageSerializer = messageSerializer;
        this.port = port;
    }

    /**
     * Initializes and starts the server. Loops until the the server should be
     * closed.
     */
    @Override
    public void run() {
        //Options options = new Options();

        running = initializeServer();

        if (serverSocket != null) {
            while (isRunning()) {
                try {
                    Socket client = serverSocket.accept();
                    ClientConnection connection =
                            new ClientConnection(this, client, storage,
                                    protocol, messageSerializer);
                    new Thread(connection).start();

                    logger.info("Connected to "
                            + client.getInetAddress().getHostName()
                            + " on port " + client.getPort());
                } catch (IOException e) {
                    logger.error("Error! " +
                            "Unable to establish connection. \n", e);
                }
            }
        }
        logger.info("Server stopped.");
    }

    public boolean isRunning() {
        return this.running;
    }

    /**
     * Stops the server insofar that it won't listen at the given port any
     * more.
     */
    private void stopServer() {
        running = false;
        try {
            serverSocket.close();
        } catch (IOException e) {
            logger.error("Error! " +
                    "Unable to close socket on port: " + port, e);
        }
    }

    private boolean initializeServer() {
        logger.info("Initialize server ...");
        try {
            serverSocket = new ServerSocket(port);
            logger.info("Server listening on port: "
                    + serverSocket.getLocalPort());
            return true;

        } catch (IOException e) {
            logger.error("Error! Cannot open server socket:");
            if (e instanceof BindException) {
                logger.error("Port " + port + " is already bound!");
            }
            return false;
        }
    }

    /**
     * Main entry point for the server application.
     */
    public static void main(String[] args) {
        try {
            // create the command line parser
            CommandLineParser parser = new DefaultParser();

            // create the Options
            Options options = new Options();
            addOption(options, "p", "port", true,
                    "port number", true);
            addOption(options, "d", "dataPath", true,
                    "path to data folder", false);
            addOption(options, "s", "cacheSize", true,
                    "the capacity of the cache", false);
            addOption(options, "c", "cacheStrategy", true,
                    "the type of cache: FIFO | None | LRU", false);
            addOption(options, "h", "help", false,
                    "see the help menu", false);
            addOption(options, "l", "logLevel", true,
                    "log level of the server: ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF",
                    false);

            int port;
            int cacheSize;
            CacheStrategy cacheStrategy;
            HelpFormatter formatter = new HelpFormatter();
            Level logLevel;
            String rootPath;

            try {
                if (args.length == 1 &&
                        (args[0].equals("-h") || args[0].equals("--help"))) {
                    formatter.printHelp("m1-server", options);
                    System.exit(1);
                    return;
                }

                CommandLine cmd = parser.parse(options, args);

                if (cmd.hasOption('h')) {
                    formatter.printHelp("m1-server", options);
                    System.exit(1);
                    return;
                }

                if (!cmd.hasOption("p")) {
                    System.out.println("Using default port " + DEFAULT_PORT);
                }
                if (!cmd.hasOption("d")) {
                    System.out.println(
                            "Using default data path '" + DEFAULT_DATA_PATH + "'");
                }

                cacheSize = Integer.parseInt(cmd.getOptionValue("s",
                        DEFAULT_CACHE_SIZE));
                if (cacheSize <= 0) {
                    throw new IllegalArgumentException(
                            "Invalid cache size: " + cacheSize);
                }

                rootPath = cmd.getOptionValue("d", DEFAULT_DATA_PATH);
                port = Integer.parseInt(cmd.getOptionValue("p", DEFAULT_PORT));
                cacheStrategy = CacheStrategy
                        .fromString(cmd.getOptionValue("c",
                                DEFAULT_CACHE_STRATEGY));

                logLevel = Level
                        .toLevel(cmd.getOptionValue("l", DEFAULT_LOG_LEVEL));
            } catch (Exception e) {
                e.printStackTrace();
                formatter.printHelp("m1-server", options);
                System.exit(1);
                return;
            }

            new LogSetup("logs/server.log", logLevel);

            KeyHashStrategy keyHashStrategy = null;

            try {
                keyHashStrategy = new MD5PrefixKeyHashStrategy(1);
            } catch (NoSuchAlgorithmException e) {
                System.out.println("Error! MD5 key hash strategy unsupported!");
                e.printStackTrace();
                System.exit(1);
            }

            IKVStorage storage = new KVStorage(rootPath, keyHashStrategy,
                    cacheSize, cacheStrategy);
            IProtocol protocol = new Protocol();
            ISerializer<KVMessage> messageSerializer = new KVMessageSerializer();

            new KVServer(storage, protocol, messageSerializer, port)
                    .start();

        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        } catch (NumberFormatException nfe) {
            System.out.println("Error! Invalid argument <port>! Not a number!");
            System.out.println("Usage: Server <port>!");
            System.exit(1);
        }
    }

    private static void addOption(Options options, String opt, String longOpt,
                                  boolean hasArg, String description,
                                  boolean required) {
        Option option = new Option(opt, longOpt, hasArg,
                description);
        option.setRequired(required);
        options.addOption(option);
    }

    @Override
    public int getPort() {
        return this.port;
    }

    /**
     * Starts the KVServer, all client requests and all ECS requests are
     * processed.
     */
    public void startServing() {
        throw new Error("Not implemented");
    }

    /**
     * Stops the KVServer, all client requests are rejected and only ECS
     * requests are processed.
     */
    public void stopServing() {
        throw new Error("Not implemented");
    }

    /**
     * Exits the KVServer application.
     */
    public void shutDown() {
        throw new Error("Not implemented");
    }

    /**
     * Lock the KVServer for write operations.
     */
    public void lockWrite() {
        throw new Error("Not implemented");
    }

    /**
     * Unlock the KVServer for write operations.
     */
    public void unlockWrite() {
        throw new Error("Not implemented");
    }

    /**
     * Transfer a subset (range) of the KVServer’s data to another KVServer
     * (reallocation before removing this server or adding a new KVServer to the
     * ring); send a notification to the ECS, if data transfer is completed.
     */
    public void moveData(String hashRangeStart,
                         String hashRangeEnd,
                         String host,
                         int port) {
        throw new Error("Not implemented");
    }

    /**
     * Update the metadata repository of this server
     */
    public void updateMetadata(Metadata metadata) {
        throw new Error("Not implemented");
    }
}
