package app_kvServer;

import app_kvECS.ZooKeeperService;
import client.ServerConnection;
import ecs.ECSController;
import logger.LogSetup;
import org.apache.commons.cli.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import server.*;
import shared.Util;
import shared.*;
import shared.messages.KVMessage;
import shared.messages.KVMessageSerializer;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class KVServer extends Thread implements IKVServer {

    private static class LockInfo {
        public ClientConnection client;
        public long time;

        public LockInfo(ClientConnection client, long time) {
            this.client = client;
            this.time = time;
        }
    }

    private static Logger logger = Logger.getRootLogger();
    private static final String DEFAULT_CACHE_SIZE = "8192";
    private static final String DEFAULT_CACHE_STRATEGY = "FIFO";
    private static final String DEFAULT_PORT = "8080";
    private static final String DEFAULT_DATA_PATH = "data";
    private static final String DEFAULT_LOG_LEVEL = "INFO";

    private int port;
    private ServerSocket serverSocket;
    private AtomicBoolean running = new AtomicBoolean(false);
    private final IKVStorage storage;
    private final IProtocol protocol;
    private final ISerializer<KVMessage> messageSerializer;

    private final ZooKeeperService zooKeeperService;

    private String name;

    private final Lock lockedKeysMutex;

    public final AtomicBoolean serving = new AtomicBoolean(false);
    public final AtomicBoolean writeLock = new AtomicBoolean(false);

    public final AtomicBoolean selfWriteLock = new AtomicBoolean(false);

    public final AtomicReference<Metadata> metadata = new AtomicReference<>(
            null);

    private final Set<ClientConnection> clientConnections = new HashSet<>();

    private HashMap<String, LockInfo> lockedKeys = new HashMap<>();

    private final Replicator replicator;

    private final TimeoutChecker timeoutChecker;

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
                    int port, String name, ZooKeeperService zooKeeperService) {
        this.lockedKeysMutex = new ReentrantLock();
        this.name = name;
        this.storage = storage;
        this.protocol = protocol;
        this.messageSerializer = messageSerializer;
        this.port = port;
        this.zooKeeperService = zooKeeperService;

        this.replicator = new Replicator(protocol, messageSerializer,
                this,
                storage);
        this.replicator.start();

        this.timeoutChecker = new TimeoutChecker(this);
        this.timeoutChecker.start();
    }

    public Replicator getReplicator() {
        return replicator;
    }

    /**
     * Initializes and starts the server. Loops until the the server should be
     * closed.
     */
    @Override
    public void run() {
        //Options options = new Options();

        running.set(initializeServer());

        if (serverSocket != null) {
            String node = String
                    .format("%s/%s", ECSController.ZOO_KEEPER_ROOT, name);

            // Notify ECS about server starting
            if (zooKeeperService != null) {
                try {
                    zooKeeperService.createNode(node, new byte[0], true);
                } catch (Exception e) {
                    logger.error("Failed to write ZooKeeperNode", e);
                    return;
                }
            }

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
                    if (isRunning()) {
                        logger.error("Error! " +
                                "Unable to establish connection. \n", e);
                    }
                }
            }
        }
        running.set(false);
        logger.info("Server stopped.");
    }

    public void addLockedKey(String key, ClientConnection client) {
        lockedKeysMutex.lock();
        try {
            lockedKeys
                    .put(key, new LockInfo(client, System.currentTimeMillis()));
        } finally {
            lockedKeysMutex.unlock();
        }
    }

    public boolean isKeyLocked(String key, ClientConnection client) {
        lockedKeysMutex.lock();
        try {
            ClientConnection value = lockedKeys.get(key).client;
            if (value == null || value == client) {
                return false;
            } else {
                return true;
            }
        } finally {
            lockedKeysMutex.unlock();
        }
    }

    public void unlockKeys(ClientConnection client) {
        lockedKeysMutex.lock();
        try {
            lockedKeys.entrySet().removeIf(e -> e.getValue().client == client);
        } finally {
            lockedKeysMutex.unlock();
        }
    }

    public void checkLockTimeout(long timeoutPeriod) {
        lockedKeysMutex.lock();
        try {
            Set<String> keys = lockedKeys.keySet();
            for (Object k : keys) {
                if (System.currentTimeMillis() - lockedKeys
                        .get(k).time > timeoutPeriod) {
                    lockedKeys.get(k).client.setInTransaction(false);
                    lockedKeys.remove(k);
                }
            }
        } finally {
            lockedKeysMutex.unlock();
        }
    }

    public boolean isRunning() {
        return this.running.get();
    }

    public String getNodeName() {
        return name;
    }

    public void registerClientConnection(ClientConnection connection) {
        synchronized (clientConnections) {
            clientConnections.add(connection);
        }
    }

    public void unregisterClientConnection(ClientConnection connection) {
        synchronized (clientConnections) {
            clientConnections.remove(connection);
        }
    }

    public void disconnectClientConnections() {
        synchronized (clientConnections) {
            for (ClientConnection connection : clientConnections) {
                connection.disconnect();
            }
        }
    }

    /**
     * Stops the server insofar that it won't listen at the given port any
     * more.
     */
    private void stopServer() {
        if (!running.get()) return;

        replicator.shutdown();

        running.set(false);
        try {
            serverSocket.close();
            disconnectClientConnections();
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
            addOption(options, "z", "zooKeeper", true,
                    "the url of zooKeeper",
                    true);
            addOption(options, "n", "name", true,
                    "name of the server",
                    true);

            int port;
            int cacheSize;
            CacheStrategy cacheStrategy;
            HelpFormatter formatter = new HelpFormatter();
            Level logLevel;
            String rootPath;
            String zooKeeperUrl;
            String name;

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
                            "Using default data path '" + DEFAULT_DATA_PATH +
                                    "'");
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

                zooKeeperUrl = cmd.getOptionValue("z");
                name = cmd.getOptionValue("n");
            } catch (Exception e) {
                e.printStackTrace();
                formatter.printHelp("m1-server", options);
                System.exit(1);
                return;
            }

            new LogSetup(String.format("logs/server_%s.log", name), logLevel);

            KeyHashStrategy keyHashStrategy = null;

            try {
                keyHashStrategy = new MD5PrefixKeyHashStrategy(1);
            } catch (NoSuchAlgorithmException e) {
                System.out.println("Error! MD5 key hash strategy unsupported!");
                e.printStackTrace();
                System.exit(1);
            }

            IKVStorage storage = new KVStorage(
                    Paths.get(rootPath, name).toString(), keyHashStrategy,
                    cacheSize, cacheStrategy);
            IProtocol protocol = new Protocol();
            ISerializer<KVMessage> messageSerializer = new KVMessageSerializer();
            ZooKeeperService zooKeeperService = new ZooKeeperService(
                    zooKeeperUrl);

            new KVServer(storage, protocol, messageSerializer, port,
                    name, zooKeeperService)
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
        //After the server has been initialized,
        //the ECS can start the server (call start()).
        serving.set(true);
    }

    /**
     * Stops the KVServer, all client requests are rejected and only ECS
     * requests are processed.
     */
    public void stopServing() {
        serving.set(false);
    }

    /**
     * Exits the KVServer application.
     */
    public void shutDown() {
        stopServer();
    }

    /**
     * Lock the KVServer for write operations.
     */
    public void lockWrite() {
        writeLock.set(true);
    }

    /**
     * Unlock the KVServer for write operations.
     */
    public void unlockWrite() {
        writeLock.set(false);
    }

    public void lockSelfWrite() {
        selfWriteLock.set(true);
    }

    /**
     * Unlock the KVServer for write operations.
     */
    public void unlockSelfWrite() {
        selfWriteLock.set(false);
    }

    /**
     * Transfer a subset (range) of the KVServer’s data to another KVServer
     * (reallocation before removing this server or adding a new KVServer to the
     * ring); send a notification to the ECS, if data transfer is completed.
     *
     * @return if the operation was successful.
     */
    public boolean copyDataTo(String hashRangeStart,
                              String hashRangeEnd,
                              String address,
                              int port) {
        List<String> keys = null;
        try {
            keys = storage.getAllKeys(hashRangeStart, hashRangeEnd);
        } catch (IOException e) {
            logger.error(e);
            return false;
        }
        // Make new server connection to successor
        ServerConnection connection = new ServerConnection(protocol,
                messageSerializer, address, port);
        try {
            try {
                // NOTE: We ignore metadata for now
                connection.connect();
            } catch (Exception e) {
                logger.error("Failed to connect with target server");
                return false;
            }
            for (String key : keys) {
                String value;
                try {
                    value = storage.get(key);
                } catch (IOException e) {
                    logger.error("Internal server error: " +
                            Util.getStackTraceString(e));
                    return false;
                }
                try {
                    int requestId = connection
                            .sendRequest(key, value,
                                    KVMessage.StatusType.ECS_PUT);
                    if (requestId == -1) {
                        logger.error(String.format(
                                "Failed to send put request (key: %s) to target server",
                                key));
                        return false;
                    }
                    try {
                        KVMessage resMessage = connection
                                .receiveMessage(requestId);
                        KVMessage.StatusType resStatus = resMessage.getStatus();
                        if (resStatus == KVMessage.StatusType.PUT_SUCCESS ||
                                resStatus == KVMessage.StatusType.PUT_UPDATE) {
                            // Success
                        } else {
                            logger.error(
                                    "Failed to send data to next server: response status = " +
                                            resStatus
                                                    .toString());
                            return false;
                        }
                    } catch (Exception e) {
                        logger.error(
                                "Failed to receive put response from target server");
                        return false;
                    }
                } catch (Exception e) {
                    logger.error(String.format(
                            "Failed to send put request (key: %s) to target server",
                            key));
                    return false;
                }
            }

            logger.info("Successfully sent all data to target server.");
            return true;
        } finally {
            connection.disconnect();
        }
    }

    public boolean deleteData(String hashRangeStart, String hashRangeEnd) {
        List<String> keys = null;
        try {
            keys = storage.getAllKeys(hashRangeStart, hashRangeEnd);
        } catch (IOException e) {
            logger.error(e);
            return false;
        }
        boolean success = true;
        for (String key : keys) {
            try {
                storage.put(key, null);
            } catch (Exception e) {
                logger.error(
                        String.format("Failed to delete tuple (key %s)", key));
                success = false;
            }
        }
        logger.info("Tuples deleted.");
        return success;
    }

    /**
     * Update the metadata repository of this server
     */
    public void updateMetadata(Metadata metadata) {
        this.metadata.set(metadata);
    }
}
