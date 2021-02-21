package client;

import org.apache.log4j.Logger;
import shared.*;
import shared.messages.KVMessage;
import shared.messages.KVMessageImpl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * NOTE: Currently this class only supports one pending message at a time. This
 * means if sendRequest is called, receiveMessage must be called before the next
 * sendRequest can be called.
 */
public class ServerConnection {
    private Logger logger = Logger.getRootLogger();

    private static final int DEFAULT_SOCKET_TIMEOUT_SECONDS = 30;

    public Thread watcher = null;
    public final AtomicBoolean terminated = new AtomicBoolean(false);
    public Socket clientSocket = null;
    public OutputStream output = null;
    public InputStream input = null;
    public boolean running;

    private IProtocol protocol;
    private ISerializer<KVMessage> serializer;
    private final BlockingQueue<Response> watcherQueue = new LinkedBlockingQueue<>();
    private String address;
    private int port;

    private boolean neverConnected = true;

    private int nextID = 0;

    public ServerConnection(IProtocol protocol,
                            ISerializer<KVMessage> serializer,
                            String address,
                            int port) {
        this.protocol = protocol;
        this.serializer = serializer;
        this.address = address;
        this.port = port;
    }

    public boolean isConnectionValid() {
        return running && !terminated.get();
    }

    public boolean isNeverConnected() {
        return neverConnected;
    }

    public Metadata connect() throws Exception {
        if (running) {
            throw new IllegalStateException("Already connected");
        }

        neverConnected = false;

        try {
            clientSocket = new Socket(address, port);
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
        Metadata metadata = null;
        try {
            Response res = protocol.readResponse(input);
            try {
                int status = res.getStatus();
                if (status == Response.Status.CONNECTION_ESTABLISHED) {
                    byte[] msgByte = res.getBody();
                    if (msgByte == null) {
                        throw new IllegalStateException(
                                "Connection error: server did not acknowledge connection");
                    }
                    KVMessage message = serializer.decode(msgByte);
                    if (message.getStatus() != KVMessage.StatusType.CONNECTED) {
                        throw new IllegalStateException(
                                "Connection error: server did not acknowledge connection");
                    }
                    metadata = message.getMetadata();
                    logger.info("Connection Established!");
                } else {
                    throw new IllegalStateException(
                            "Connection error: server did not acknowledge connection");
                }
            } catch (Exception e) {
                logger.error("Connection Error!");
                disconnect();
                throw e;
            }
        } catch (IOException ioe) {
            logger.error("Connection lost!");
            disconnect();
            throw ioe;
        }

        terminated.set(false);
        watcherQueue.clear();
        watcher = new Thread(
                new SocketWatcher(protocol, watcherQueue, this));
        watcher.start();
        return metadata;
    }

    public void disconnect() {
        if (!running) {
            return;
        }

        logger.info("trying to close connection ...");

        running = false;

        try {
            logger.info("tearing down the connection ...");

            if (clientSocket != null) {
                // This must be after setting running = false to avoid infinite recursion
                sendRequest(null, null, KVMessage.StatusType.DISCONNECT);

                clientSocket.close();
            }
        } catch (IOException ioe) {
            logger.error("Unable to close socket!", ioe);
        } finally {
            clientSocket = null;
            if (watcher != null) {
                try {
                    watcher.join();
                } catch (InterruptedException e) {
                    logger.error(Util.getStackTraceString(e));
                }
            }
            terminated.set(false);
            watcher = null;
            watcherQueue.clear();
            logger.info("connection closed.");
        }
    }

    public KVMessage receiveMessage(int requestID) throws Exception {
        return receiveMessage(requestID, DEFAULT_SOCKET_TIMEOUT_SECONDS);
    }

    public KVMessage receiveMessage(int requestID, int timeoutSeconds) throws
            Exception {
        while (true) {
            Response res = watcherQueue
                    .poll(timeoutSeconds, TimeUnit.SECONDS);
            if (res == null) {
                return new KVMessageImpl(null, "Request timed out.",
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
                    logger.debug("Received message: " + message.toString());
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
     * See {@link #sendRequest(KVMessage message)}.
     */
    public int sendRequest(String key, String value,
                           KVMessage.StatusType status) throws IOException {
        return sendRequest(new KVMessageImpl(key, value, status));
    }

    /**
     * Return the ID of the request sent. If request failed to send, return -1.
     * Note that ID given must be non-negative.
     */
    public int sendRequest(KVMessage message) throws IOException {
        try {
            logger.debug("Sending message: " + message.toString());
            byte[] msgBytes;
            try {
                msgBytes = serializer.encode(message);
            } catch (Exception e) {
                logger.error("Failed to serialize message: " + Util
                        .getStackTraceString(e));
                return -1;
            }
            int id = nextID;
            protocol.writeRequest(output, id, msgBytes);
            nextID++;
            return id;
        } catch (IOException e) {
            logger.warn("Unable to send message! Disconnected!");
            disconnect();
            throw e;
        }
    }


}
