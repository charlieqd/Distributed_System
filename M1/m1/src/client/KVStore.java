package client;

import org.apache.log4j.Logger;
import shared.IProtocol;
import shared.Protocol;
import shared.Response;
import shared.Util;
import shared.messages.KVMessage;
import shared.messages.KVMessageImpl;
import shared.messages.KVMessageSerializer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class KVStore implements KVCommInterface {

    private static class ServerConnection {
        private BlockingQueue<Response> watcherQueue = new LinkedBlockingQueue<>();
        private Thread watcher = null;
        private final AtomicBoolean terminated = new AtomicBoolean(false);
    }

    private static class SocketWatcher implements Runnable {
        private Logger logger = Logger.getRootLogger();

        private IProtocol protocol;
        private KVStore store;
        private InputStream socketInput;
        private ServerConnection connection;

        public SocketWatcher(IProtocol protocol,
                             KVStore store,
                             InputStream socketInput,
                             ServerConnection connection) {
            this.protocol = protocol;
            this.store = store;
            this.socketInput = socketInput;
            this.connection = connection;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    Response res = protocol.readResponse(socketInput);
                    connection.watcherQueue.put(res);
                } catch (IOException | InterruptedException e) {
                    logger.info("Socket closed.");
                    try {
                        connection.watcherQueue
                                .put(new Response(new byte[0], -1,
                                        Response.Status.DISCONNECTED));
                    } catch (InterruptedException interruptedException) {
                        logger.error(
                                Util.getStackTraceString(interruptedException));
                    }
                    connection.terminated.set(true);
                    store.notifyStatusChange(
                            KVStoreListener.SocketStatus.DISCONNECTED);
                    break;
                }
            }
        }
    }

    /**
     * Initialize KVStore with address and port of KVServer
     *
     * @param address the address of the KVServer
     * @param port    the port of the KVServer
     */

    private Logger logger = Logger.getRootLogger();
    private final Set<KVStoreListener> listeners;
    private boolean running;

    private Socket clientSocket;
    private OutputStream output;
    private InputStream input;

    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 1024 * BUFFER_SIZE;
    private static final int SOCKET_TIMEOUT_SECONDS = 30;
    private String serverAddress;
    private int serverPort;
    private int nextId;

    private KVMessageSerializer serializer;
    private Protocol protocol;

    private List<ServerConnection> connections = new ArrayList<>();


    public KVStore(String address, int port) throws IOException {
        serverAddress = address;
        serverPort = port;
        nextId = 0;
        listeners = new HashSet<KVStoreListener>();
        serializer = new KVMessageSerializer();
        protocol = new Protocol();
        watcherQueue = new LinkedBlockingQueue<>();
        connectionTerminated = new AtomicBoolean();
    }

    public boolean isConnectionValid() {
        if (!running) return false;

        for (ServerConnection connection : connections) {
            if (!connection.terminated.get()) {
                return true;
            }
        }
        return false;
    }

    private KVMessage receiveMessage(int requestID) throws Exception {
        while (true) {
            Response res = watcherQueue
                    .poll(SOCKET_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (res == null) {
                return new KVMessageImpl(null, "Reqeust timed out.",
                        KVMessage.StatusType.FAILED);
            }

            int status = res.getStatus();
            if (status == Response.Status.OK) {
                byte[] msgByte = res.getBody();
                int id = res.getId();
                if (id != requestID) {
                    // Skip old timed-out responses
                    continue;
                }

                try {
                    KVMessage message = serializer.decode(msgByte);
                    logger.info("Received message: " + message.toString());
                    return message;
                } catch (Exception e) {
                    logger.warn("Failed to decode message", e);
                    return new KVMessageImpl(null,
                            "Failed to decode message",
                            KVMessage.StatusType.FAILED);
                }
            } else if (status == Response.Status.DISCONNECTED) {
                return new KVMessageImpl(null,
                        "Request failed: disconnected.",
                        KVMessage.StatusType.FAILED);
            } else {
                return new KVMessageImpl(null,
                        "Incorrect response status: " +
                                Integer.toString(status),
                        KVMessage.StatusType.FAILED);
            }
        }
    }

    /**
     * Return the ID of the request sent. If request failed to send, return -1.
     */
    private int sendRequest(String key, String value,
                            KVMessage.StatusType status) throws IOException {
        try {
            KVMessage kvMsg = new KVMessageImpl(key, value, status);
            logger.info("Sending message: " + kvMsg.toString());
            byte[] msgBytes;
            try {
                msgBytes = serializer.encode(kvMsg);
            } catch (Exception e) {
                logger.error("Failed to serialize message: " + Util
                        .getStackTraceString(e));
                return -1;
            }
            int id = nextId;
            protocol.writeRequest(output, nextId, msgBytes);
            nextId += 1;
            return id;
        } catch (IOException e) {
            logger.warn("Unable to send message! Disconnected!");
            disconnect();
            throw e;
        }
    }


    @Override
    public void connect() throws Exception {
        if (running) {
            throw new IllegalStateException("Already connected");
        }

        try {
            clientSocket = new Socket(serverAddress, serverPort);
            output = clientSocket.getOutputStream();
            input = clientSocket.getInputStream();
        } catch (Exception e) {
            logger.warn("Socket creation failed", e);
            clientSocket = null;
            output = null;
            input = null;
            throw e;
        }

        running = true;

        try {
            Response res = protocol.readResponse(input);
            int status = res.getStatus();
            if (status == Response.Status.CONNECTION_ESTABLISHED) {
                logger.info("Connection Established!");
            } else {
                logger.error("Connection Error!");
                disconnect();
                throw new IllegalStateException(
                        "Connection error: server did not acknowledge connection");
            }
        } catch (IOException ioe) {
            logger.error("Connection lost!");
            disconnect();
            throw ioe;
        }

        connectionTerminated.set(false);
        watcherQueue.clear();
        watcher = new Thread(
                new SocketWatcher(protocol, this, input, watcherQueue));
        watcher.start();
    }

    @Override
    public void disconnect() {
        if (!running) {
            return;
        }

        logger.info("trying to close connection ...");

        try {
            running = false;

            logger.info("tearing down the connection ...");

            if (clientSocket != null) {
                // This must be after setting running = false to avoid infinite recursion
                sendRequest(null, null, KVMessage.StatusType.DISCONNECT);

                clientSocket.close();
                clientSocket = null;
                try {
                    watcher.join();
                } catch (InterruptedException e) {
                    logger.error(Util.getStackTraceString(e));
                }
                connectionTerminated.set(false);
                watcher = null;
                watcherQueue.clear();
                logger.info("connection closed!");
            }
        } catch (IOException ioe) {
            logger.error("Unable to close connection!");
        }
    }

    @Override
    public KVMessage put(String key, String value) throws Exception {
        int id = sendRequest(key, value, KVMessage.StatusType.PUT);
        if (id < 0) {
            return new KVMessageImpl(null, "Failed to send request.",
                    KVMessage.StatusType.FAILED);
        }
        return receiveMessage(id);
    }

    @Override
    public KVMessage get(String key) throws Exception {
        int id = sendRequest(key, null, KVMessage.StatusType.GET);
        if (id < 0) {
            return new KVMessageImpl(null, "Failed to send request.",
                    KVMessage.StatusType.FAILED);
        }
        return receiveMessage(id);
    }

    public void addListener(KVStoreListener listener) {
        synchronized (listeners) {
            listeners.add(listener);
        }
    }

    public void removeListener(KVStoreListener listener) {
        synchronized (listeners) {
            listeners.remove(listener);
        }
    }

    private void notifyStatusChange(KVStoreListener.SocketStatus status) {
        synchronized (listeners) {
            for (KVStoreListener listener : listeners) {
                listener.handleStatusChange(status);
            }
        }
    }
}
