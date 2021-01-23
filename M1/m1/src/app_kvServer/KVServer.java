package app_kvServer;

import logger.LogSetup;
import org.apache.commons.cli.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import server.*;
import shared.IProtocol;
import shared.ISerializer;
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
                            new ClientConnection(client, storage, protocol,
                                    messageSerializer);
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

    private boolean isRunning() {
        return this.running;
    }

    /**
     * Stops the server insofar that it won't listen at the given port any
     * more.
     */
    public void stopServer() {
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
     * Main entry point for the echo server application.
     *
     * @param args contains the port number at args[0].
     */
    public static void main(String[] args) {
        try {
            // TODO implement path and other params
            String rootPath = "data";

            KeyHashStrategy keyHashStrategy = null;

            // create the command line parser
            CommandLineParser parser = new DefaultParser();

            // create the Options
            Options options = new Options();
            addOption(options, "s", "cacheSize", true,
                    "the capacity of the cache", false);
            addOption(options, "p", "port", true,
                    "port number", false);
            addOption(options, "c", "cacheStrategy", true,
                    "the type of cache: FIFO | None | LRU", false);
            addOption(options, "h", "help", false,
                    "see the help menu", false);

            int port;
            int cacheSize;
            CacheStrategy cacheStrategy;
            HelpFormatter formatter = new HelpFormatter();

            try {
                CommandLine cmd = parser.parse(options, args);

                if (cmd.hasOption('h')) {
                    formatter.printHelp("m1-server", options);
                    System.exit(1);
                    return;
                }

                if (!cmd.hasOption("p")) {
                    System.out.println("Using default port " + DEFAULT_PORT);
                }

                cacheSize = Integer.parseInt(cmd.getOptionValue("s",
                        DEFAULT_CACHE_SIZE));
                port = Integer.parseInt(cmd.getOptionValue("p", DEFAULT_PORT));
                cacheStrategy = CacheStrategy
                        .fromString(cmd.getOptionValue("c", "FIFO"));
            } catch (Exception e) {
                e.printStackTrace();
                formatter.printHelp("m1-server", options);
                System.exit(1);
                return;
            }

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
            new LogSetup("logs/server.log", Level.ALL);

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
        // TODO Auto-generated method stub
        return -1;
    }

    @Override
    public String getHostname() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public CacheStrategy getCacheStrategy() {
        // TODO Auto-generated method stub
        return IKVServer.CacheStrategy.None;
    }

    @Override
    public int getCacheSize() {
        // TODO Auto-generated method stub
        return -1;
    }

    @Override
    public boolean inStorage(String key) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean inCache(String key) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public String getKV(String key) throws Exception {
        // TODO Auto-generated method stub
        return "";
    }

    @Override
    public void putKV(String key, String value) throws Exception {
        // TODO Auto-generated method stub
    }

    @Override
    public void clearCache() {
        // TODO Auto-generated method stub
    }

    @Override
    public void clearStorage() {
        // TODO Auto-generated method stub
    }

    @Override
    public void kill() {
        // TODO Auto-generated method stub
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub
    }
}
