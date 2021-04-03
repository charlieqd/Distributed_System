package client;

import org.apache.log4j.Logger;
import shared.ECSNode;
import shared.Metadata;
import shared.Protocol;
import shared.Util;
import shared.messages.KVMessage;
import shared.messages.KVMessageImpl;
import shared.messages.KVMessageSerializer;

import java.io.IOException;
import java.util.*;

public class KVStore implements KVCommInterface {

    public static final int MAX_NUM_ATTEMPTS = 10;
    private static final int MAX_TRANSACTION_RETRIES = 5;
    private static final int TRANSACTION_RETRY_DELAY_MILLIS = 500;

    private Logger logger = Logger.getRootLogger();

    private String initialAddress;
    private int initialPort;

    private KVMessageSerializer serializer;
    private Protocol protocol;

    /**
     * Map from ring position to connection helper
     */
    private Map<String, ServerConnection> connections = new HashMap<>();
    private Metadata cachedMetadata = null;

    private boolean transactionRunning = false;

    private Map<String, ServerConnection> transactionKeyToConnection = new HashMap<>();
    private Set<ServerConnection> transactionConnections = new HashSet<>();

    public KVStore(String initialAddress, int initialPort) throws IOException {
        this.initialAddress = initialAddress;
        this.initialPort = initialPort;
        serializer = new KVMessageSerializer();
        protocol = new Protocol();
    }

    public boolean isConnectionValid() {
        return !connections.isEmpty();
        // for (String ringPosition : connections.keySet()) {
        //     ServerConnection connection = connections.get(ringPosition);
        //     if (connection.isConnectionValid()) {
        //         return true;
        //     }
        // }
        // return false;
    }

    @Override
    public void connect() throws Exception {
        ServerConnection connection = new ServerConnection(protocol,
                serializer, initialAddress, initialPort);
        Metadata metadata = connection.connect();
        // NOTE: If connection failed, it will throw exception upward.
        connections.put("", connection);
        processNewMetadata(metadata);
    }

    @Override
    public void disconnect() {
        for (ServerConnection connection : connections.values()) {
            connection.disconnect();
        }
        connections.clear();
    }

    @Override
    public KVMessage put(String key, String value) throws Exception {
        if (transactionRunning) {
            throw new IllegalStateException(
                    "Transaction running. Use transactionPut instead.");
        }
        return sendRequest(key, value, KVMessage.StatusType.PUT);
    }

    @Override
    public KVMessage get(String key) throws Exception {
        if (transactionRunning) {
            throw new IllegalStateException(
                    "Transaction running. Use transactionGet instead.");
        }
        return sendRequest(key, null, KVMessage.StatusType.GET);
    }

    @Override
    public KVMessage transactionPut(String key, String value) throws Exception {
        if (transactionRunning) {
            throw new IllegalStateException(
                    "Transaction not running. Use put instead.");
        }
        return sendTransactionRequest(key, value,
                KVMessage.StatusType.TRANSACTION_PUT);
    }

    @Override
    public KVMessage transactionGet(String key) throws Exception {
        if (transactionRunning) {
            throw new IllegalStateException(
                    "Transaction not running. Use get instead.");
        }
        return sendTransactionRequest(key, null,
                KVMessage.StatusType.TRANSACTION_GET);
    }

    @Override
    public void runTransaction(TransactionRunner runner) throws Exception {
        if (transactionRunning) {
            throw new IllegalStateException("Transaction already running");
        }
        try {
            transactionRunning = true;
            int retries = 0;
            while (retries < MAX_TRANSACTION_RETRIES) {
                retries++;
                try {
                    transactionKeyToConnection.clear();
                    transactionConnections.clear();

                    runner.run(this);

                    for (ServerConnection connection : transactionConnections) {
                        sendTransactionMetaRequest(connection,
                                KVMessage.StatusType.TRANSACTION_COMMIT);
                        // TODO more fault tolerance
                    }

                    return;
                } catch (Exception e) {
                    logger.warn("Error while attempting transaction.", e);

                    for (ServerConnection connection : transactionConnections) {
                        sendTransactionMetaRequest(connection,
                                KVMessage.StatusType.TRANSACTION_ROLLBACK);
                        // TODO more fault tolerance
                    }

                    if (e instanceof RetryTransactionException) {
                        // Retry

                        transactionKeyToConnection.clear();
                        transactionConnections.clear();

                        if (retries < MAX_TRANSACTION_RETRIES) {
                            logger.info("Retrying transaction.");
                        }
                    } else {
                        throw e;
                    }
                }
            }
            throw new IllegalStateException(
                    "Max number of transaction retries exceeded");
        } finally {
            transactionRunning = false;

            transactionKeyToConnection.clear();
            transactionConnections.clear();
        }
    }

    private KVMessage sendRequest(String key,
                                  String value,
                                  KVMessage.StatusType status) throws
            Exception {
        int attemptCount = 0;
        while (attemptCount < MAX_NUM_ATTEMPTS) {
            attemptCount++;

            ServerConnection connection = getOrCreateServerConnection(key);
            if (connection == null) {
                disconnect();
                throw new IOException("Request failed: disconnected.");
            }
            int id = -1;
            try {
                id = connection.sendRequest(key, value, status);
            } catch (IOException e) {
                connection.disconnect();
                // We re-try the connection; the invalid connection will be removed.
                continue;
            }
            if (id == -1) {
                return new KVMessageImpl(null, "Failed to send request.",
                        KVMessage.StatusType.FAILED);
            }
            KVMessage message = connection.receiveMessage(id);
            KVMessage.StatusType resStatus = message.getStatus();
            if (resStatus == KVMessage.StatusType.NOT_RESPONSIBLE) {
                processNewMetadata(message.getMetadata());
                // Retry the request
                continue;
            }
            return message;
        }
        return new KVMessageImpl(null,
                "Failed to send request: Too many retries",
                KVMessage.StatusType.FAILED);
    }

    private KVMessage sendTransactionRequest(String key,
                                             String value,
                                             KVMessage.StatusType status) throws
            Exception {
        int attemptCount = 0;
        while (attemptCount < MAX_NUM_ATTEMPTS) {
            attemptCount++;

            ServerConnection connection = getOrCreateServerConnection(key);
            if (connection == null) {
                disconnect();
                throw new IOException("Request failed: disconnected.");
            }

            ServerConnection connectionForKey = transactionKeyToConnection
                    .get(key);
            if (connectionForKey == null) {
                transactionKeyToConnection.put(key, connection);
            } else if (connection != connectionForKey) {
                throw new RetryTransactionException(
                        String.format("Connection for key \"%s\" changed",
                                key));
            }
            if (!transactionConnections.contains(connection)) {
                transactionConnections.add(connection);
                sendTransactionMetaRequest(connection,
                        KVMessage.StatusType.TRANSACTION_BEGIN);
            }

            int id = -1;
            try {
                id = connection.sendRequest(key, value, status);
            } catch (IOException e) {
                connection.disconnect();
                // We re-try the connection; the invalid connection will be removed.
                continue;
            }
            if (id == -1) {
                return new KVMessageImpl(null, "Failed to send request.",
                        KVMessage.StatusType.FAILED);
            }
            KVMessage message = connection.receiveMessage(id);
            KVMessage.StatusType resStatus = message.getStatus();
            if (resStatus == KVMessage.StatusType.NOT_RESPONSIBLE) {
                processNewMetadata(message.getMetadata());
                // Retry the request
                continue;
            }
            if (resStatus == KVMessage.StatusType.SERVER_WRITE_LOCK) {
                throw new RetryTransactionException(
                        String.format("Server locked for key \"%s\"", key));
            }
            if (resStatus == KVMessage.StatusType.FAILED) {
                throw new RetryTransactionException(
                        String.format("Operation failed for key \"%s\"", key));
            }
            return message;
        }
        return new KVMessageImpl(null,
                "Failed to send request: Too many retries",
                KVMessage.StatusType.FAILED);
    }

    private String getTransactionMetaRequestErrorPrefix(KVMessage.StatusType status) {
        switch (status) {
            case TRANSACTION_BEGIN:
                return "Failed to begin transaction: ";
            case TRANSACTION_COMMIT:
                return "Failed to commit transaction: ";
            case TRANSACTION_ROLLBACK:
                return "Failed to rollback transaction: ";
            default:
                return "";
        }
    }

    private void sendTransactionMetaRequest(ServerConnection connection,
                                            KVMessage.StatusType status) throws
            Exception {
        int id = -1;
        try {
            id = connection.sendRequest(null, null,
                    KVMessage.StatusType.TRANSACTION_COMMIT);
        } catch (IOException e) {
            throw new IOException(
                    getTransactionMetaRequestErrorPrefix(status) +
                            Util.getStackTraceString(e));
        }
        if (id == -1) {
            throw new IOException(
                    getTransactionMetaRequestErrorPrefix(status) +
                            "sendRequest returned invalid ID");
        }
        KVMessage message = connection.receiveMessage(id);
        KVMessage.StatusType resStatus = message.getStatus();
        if (resStatus !=
                KVMessage.StatusType.TRANSACTION_SUCCESS) {
            throw new IOException(
                    getTransactionMetaRequestErrorPrefix(status) +
                            "received response " +
                            message.toString());
        }
    }

    /**
     * Obtain a valid server connection for the given key using current
     * metadata. If a connection to the target server does not exist, a new one
     * will be established. If this method fails to establish new connection, it
     * will return one of the other connections. If no connection is found, will
     * return null.
     */
    private ServerConnection getOrCreateServerConnection(String key) throws
            Exception {
        String ringPosition = Metadata.getRingPosition(key);
        ServerConnection connection = null;
        ECSNode info = null;
        if (cachedMetadata != null) {
            info = cachedMetadata.getServer(ringPosition);
            if (info != null) {
                ringPosition = info.getPosition();
                connection = connections.get(ringPosition);
            }
        }

        while (true) {
            if (connection != null) {
                if (connection.isNeverConnected()) {
                    try {
                        // NOTE: We ignore metadata for now
                        connection.connect();
                    } catch (Exception e) {
                        // Failed to connect
                    }
                }

                if (connection.isConnectionValid()) {
                    return connection;
                } else {
                    if (info != null) {
                        logger.info(String.format("Connection to %s invalid",
                                info.getNodeName()));
                    }
                    connections.remove(ringPosition);
                    connection = null;
                }
            } else {
                if (connections.isEmpty()) return null;
                // Attempt to connect to replica
                List<String> ringPositions = new ArrayList<>(
                        connections.keySet());
                Collections.sort(ringPositions);
                int index = ringPositions.indexOf(ringPosition);
                if (index == -1) {
                    ringPosition = ringPositions.get(0);
                } else {
                    ringPosition = ringPositions
                            .get((index + 1) % ringPositions.size());
                }
                connection = connections.get(ringPosition);
            }
        }
    }

    private void processNewMetadata(Metadata metadata) {
        // If metadata is null, that means the server who sent the metadata did
        // not know any metadata at all.
        if (metadata == null) return;

        logger.info("Received new metadata. Refreshing connections...");

        for (ServerConnection connection : connections.values()) {
            connection.disconnect();
        }
        connections.clear();

        for (ECSNode info : metadata.getServers()) {
            ServerConnection connection = new ServerConnection(protocol,
                    serializer, info.getNodeHost(), info.getNodePort());
            connections.put(info.getPosition(), connection);
        }

        cachedMetadata = metadata;
    }
}
