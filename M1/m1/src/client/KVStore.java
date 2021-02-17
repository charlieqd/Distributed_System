package client;

import org.apache.log4j.Logger;
import shared.ECSNode;
import shared.Metadata;
import shared.Protocol;
import shared.messages.KVMessage;
import shared.messages.KVMessageImpl;
import shared.messages.KVMessageSerializer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class KVStore implements KVCommInterface {

    public static final int MAX_NUM_ATTEMPTS = 10;

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
        return sendRequest(key, value, KVMessage.StatusType.PUT);
    }

    @Override
    public KVMessage get(String key) throws Exception {
        return sendRequest(key, null, KVMessage.StatusType.GET);
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
                return new KVMessageImpl(null, "Request failed: disconnected.",
                        KVMessage.StatusType.FAILED);
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
        if (cachedMetadata != null) {
            ECSNode info = cachedMetadata.getServer(ringPosition);
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
                    connections.remove(ringPosition);
                    connection = null;
                }
            } else {
                if (connections.isEmpty()) return null;
                Map.Entry<String, ServerConnection> entry =
                        connections.entrySet().iterator().next();
                ringPosition = entry.getKey();
                connection = entry.getValue();
            }
        }
    }

    private void processNewMetadata(Metadata metadata) {
        // If metadata is null, that means the server who sent the metadata did
        // not know any metadata at all.
        if (metadata == null) return;

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
