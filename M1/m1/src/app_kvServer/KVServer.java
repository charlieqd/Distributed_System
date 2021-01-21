package app_kvServer;

import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import server.ClientConnection;
import shared.IProtocol;
import shared.ISerializer;
import shared.Protocol;
import shared.messages.KVMessage;
import shared.messages.KVMessageSerializer;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;

public class KVServer extends Thread implements IKVServer {

    private static Logger logger = Logger.getRootLogger();

    private static final int DEFAULT_CACHE_SIZE = 8192;
    private static final CacheStrategy DEFAULT_STRATEGY = CacheStrategy.FIFO;

    private int port;
    private ServerSocket serverSocket;
    private boolean running;
    private final IProtocol protocol;
    private final ISerializer<KVMessage> messageSerializer;

    /**
     * Start KV Server at given port
     *
     * @param protocol          the message protocol encoder/decoder.
     * @param messageSerializer serializer for messages.
     * @param port              given port for storage server to operate
     * @param cacheSize         specifies how many key-value pairs the server is
     *                          allowed to keep in-memory
     * @param strategy          specifies the cache replacement strategy in case
     *                          the cache is full and there is a GET- or
     *                          PUT-request on a key that is currently not
     *                          contained in the cache.
     */
    public KVServer(IProtocol protocol,
                    ISerializer<KVMessage> messageSerializer,
                    int port,
                    int cacheSize,
                    CacheStrategy strategy) {
        this.protocol = protocol;
        this.messageSerializer = messageSerializer;
        this.port = port;
    }

    public KVServer(IProtocol protocol,
                    ISerializer<KVMessage> messageSerializer,
                    int port) {
        this(protocol, messageSerializer, port, DEFAULT_CACHE_SIZE,
                DEFAULT_STRATEGY);
    }

    /**
     * Initializes and starts the server. Loops until the the server should be
     * closed.
     */
    @Override
    public void run() {

        running = initializeServer();

        if (serverSocket != null) {
            while (isRunning()) {
                try {
                    Socket client = serverSocket.accept();
                    ClientConnection connection =
                            new ClientConnection(client, protocol,
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
            IProtocol protocol = new Protocol();
            ISerializer<KVMessage> messageSerializer = new KVMessageSerializer();
            new LogSetup("logs/server.log", Level.ALL);
            if (args.length != 1) {
                System.out.println("Error! Invalid number of arguments!");
                System.out.println("Usage: Server <port>!");
            } else {
                int port = Integer.parseInt(args[0]);
                new KVServer(protocol, messageSerializer, port).start();
            }
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
