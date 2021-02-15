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
    Metadata metaData = null;

    public KVStore(String address, int port) throws IOException {
        serverAddress = address;
        serverPort = port;
        nextId = 0;
        serializer = new KVMessageSerializer();
        protocol = new Protocol();
        watcherQueue = new LinkedBlockingQueue<>();
    }

    public boolean isConnectionValid() {
        throw new Error("Not implemented");
        for (ServerConnection connection : connections) {
            if (connection.isConnectionValid()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void connect() throws Exception {
        ServerConnection connection = new ServerConnection(protocol, watcherQueue,serverAddress, serverPort, serializer);
        metaData = connection.connect();
        connections.put("", connection);
        throw new Error("Not implemented");
    }

    @Override
    public void disconnect() {
        for(ServerConnection connection : connections.values()){
            connection.disconnect();
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
}
