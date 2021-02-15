package client;

import org.apache.log4j.Logger;
import shared.Protocol;
import shared.Response;
import shared.messages.KVMessage;
import shared.messages.KVMessageImpl;
import shared.messages.KVMessageSerializer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class KVStore implements KVCommInterface {

    /**
     * Initialize KVStore with address and port of KVServer
     *
     * @param address the address of the KVServer
     * @param port    the port of the KVServer
     */

    private Logger logger = Logger.getRootLogger();

    private String serverAddress;
    private int serverPort;
    private int nextId;

    private KVMessageSerializer serializer;
    private Protocol protocol;

    public BlockingQueue<Response> watcherQueue = new LinkedBlockingQueue<>();

    private Map<String, ServerConnection> connections = new HashMap<>();


    public KVStore(String address, int port) throws IOException {
        serverAddress = address;
        serverPort = port;
        nextId = 0;
        serializer = new KVMessageSerializer();
        protocol = new Protocol();
        watcherQueue = new LinkedBlockingQueue<>();
    }

    public boolean isConnectionValid() {
        for (String ringPosition : connections.keySet()) {
            ServerConnection connection = connections.get(ringPosition);
            if (connection.isConnectionValid()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void connect() throws Exception {
        throw new Error("Not implemented");
    }

    @Override
    public void disconnect() {
        throw new Error("Not implemented");
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
        while (true) {
            ServerConnection connection = getOrCreateServerConnection(key);
            int id = connection.sendRequest(
                    key, value, status);
            if (id < 0) {
                return new KVMessageImpl(null, "Failed to send request.",
                        KVMessage.StatusType.FAILED);
            }
            KVMessage message = connection.receiveMessage(id);
            KVMessage.StatusType resStatus = message.getStatus();
            if (resStatus == KVMessage.StatusType.NOT_RESPONSIBLE) {
                // TODO
                processNewMetadata(message.getMetadata());
                continue;
            }
            return message;
        }
    }

    /**
     * Obtain the server connection for the given key using current metadata. If
     * a connection to the target server does not exist, a new connection will
     * be established. If this method fails to establish new connection.
     */
    private ServerConnection getOrCreateServerConnection(String key) {
        throw new Error("Not implemented");
    }

    private void processNewMetadata(Metadata metadata) {
        throw new Error("Not implemented");
    }
}
