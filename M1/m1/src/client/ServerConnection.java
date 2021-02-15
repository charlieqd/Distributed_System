package client;

import org.apache.log4j.Logger;
import shared.IProtocol;
import shared.ISerializer;
import shared.Response;
import shared.Util;
import shared.messages.KVMessage;
import shared.messages.KVMessageImpl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ServerConnection {
    private Logger logger = Logger.getRootLogger();

    private static final int SOCKET_TIMEOUT_SECONDS = 30;

    public Thread watcher = null;
    public final AtomicBoolean terminated = new AtomicBoolean(false);
    public Socket clientSocket = null;
    public OutputStream output = null;
    public InputStream input = null;
    public boolean running;

    private IProtocol protocol;
    private ISerializer<KVMessage> serializer;
    private BlockingQueue<Response> watcherQueue;
    private String address;
    private int port;

    private int nextId = 0;

    public ServerConnection(IProtocol protocol,
                            ISerializer<KVMessage> serializer,
                            BlockingQueue<Response> watcherQueue,
                            String address,
                            int port) {
        this.protocol = protocol;
        this.serializer = serializer;
        this.watcherQueue = watcherQueue;
        this.address = address;
        this.port = port;
    }

    public boolean isConnectionValid() {
        return running && !terminated.get();
    }

    public void connect() throws Exception {
        if (running) {
            throw new IllegalStateException("Already connected");
        }

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

        terminated.set(false);
        watcherQueue.clear();
        watcher = new Thread(
                new SocketWatcher(protocol, watcherQueue, this));
        watcher.start();
    }

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
                terminated.set(false);
                watcher = null;
                watcherQueue.clear();
                logger.info("connection closed!");
            }
        } catch (IOException ioe) {
            logger.error("Unable to close connection!");
        }
    }

    public KVMessage receiveMessage(int requestID) throws Exception {
        while (true) {
            Response res = watcherQueue
                    .poll(SOCKET_TIMEOUT_SECONDS, TimeUnit.SECONDS);
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
    public int sendRequest(String key, String value,
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


}
