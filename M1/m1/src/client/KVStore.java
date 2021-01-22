package client;

import org.apache.log4j.Logger;
import shared.Protocol;
import shared.Response;
import shared.messages.KVMessage;
import shared.messages.KVMessageImpl;
import shared.messages.KVMessageSerializer;
import shared.messages.TextMessage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;

public class KVStore extends Thread implements KVCommInterface {
    /**
     * Initialize KVStore with address and port of KVServer
     *
     * @param address the address of the KVServer
     * @param port    the port of the KVServer
     */

    private Logger logger = Logger.getRootLogger();
    private Set<ClientSocketListener> listeners;
    private boolean running;

    private Socket clientSocket;
    private OutputStream output;
    private InputStream input;

    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 1024 * BUFFER_SIZE;
    private String serverAddress;
    private int serverPort;
    private int nextId;
    private Hashtable<int, Response> idToResponse;

    private KVMessageSerializer serializer;
    private Protocol protocol;


    public KVStore(String address, int port) throws IOException {
        // TODO Auto-generated method stub
        serverAddress = address;
        serverPort = port;
        nextId = 0;
        idToResponse = new Hashtable<int, Response>();
        logger.info("KVStore Initialized");
    }

    private void setRunning(boolean run) {
        running = run;
    }

    public void run() {
        try {
            output = clientSocket.getOutputStream();
            input = clientSocket.getInputStream();

            while (isRunning()) {
                try {
                    KVMessage latestMsg = receiveMessage();
                    for (ClientSocketListener listener : listeners) {
                        listener.handleNewMessage(latestMsg);
                    }
                } catch (IOException ioe) {
                    if (isRunning()) {
                        logger.error("Connection lost!");
                        try {
                            tearDownConnection();
                            for (ClientSocketListener listener : listeners) {
                                listener.handleStatus(
                                        ClientSocketListener.SocketStatus.CONNECTION_LOST);
                            }
                        } catch (IOException e) {
                            logger.error("Unable to close connection!");
                        }
                    }
                }
            }
        } catch (IOException ioe) {
            logger.error("Connection could not be established!");

        } finally {
            if (isRunning()) {
                disconnect();
            }
        }
    }

    private void tearDownConnection() throws IOException {
        setRunning(false);
        logger.info("tearing down the connection ...");
        if (clientSocket != null) {
            clientSocket.close();
            clientSocket = null;
            logger.info("connection closed!");
        }
    }

    public boolean isRunning() {
        return running;
    }

    @Override
    public void connect() throws Exception {
        // TODO Auto-generated method stub
        clientSocket = new Socket(serverAddress, serverPort);
        output = clientSocket.getOutputStream();
        input = clientSocket.getInputStream();
        listeners = new HashSet<ClientSocketListener>();
        setRunning(true);
        serializer = new KVMessageSerializer();
        protocol = new Protocol();
        logger.info("Connection established");
    }

    @Override
    public void disconnect() {
        // TODO Auto-generated method stub
        logger.info("try to close connection ...");

        try {
            tearDownConnection();
            for (ClientSocketListener listener : listeners) {
                listener.handleStatus(
                        ClientSocketListener.SocketStatus.DISCONNECTED);
            }
        } catch (IOException ioe) {
            logger.error("Unable to close connection!");
        }
    }

    @Override
    public KVMessage put(String key, String value) throws Exception {
        // TODO Auto-generated method stub
//        KVMessageSerializer s = new KVMessageSerializer();
        KVMessage kvMsg = new KVMessageImpl(key, value,
                KVMessage.StatusType.PUT);
        byte[] msgBytes = serializer.encode(kvMsg);
//        Protocol p = new Protocol();
        protocol.writeRequest(output, nextId, msgBytes);
        idToResponse.put(nextId, null);
        nextId += 1;
        /*Returns:
        a message that confirms the insertion of the tuple or an error.
        Throws:
        Exception – if put command cannot be executed (e.g. not connected to any KV server).*/
        //??
        return protocol.readResponse(input);
    }

    @Override
    public KVMessage get(String key) throws Exception {
        // TODO Auto-generated method stub
//        KVMessageSerializer serializer = new KVMessageSerializer();
        KVMessage kvMsg = new KVMessageImpl(key, null,
                KVMessage.StatusType.GET);
        byte[] msgBytes = serializer.encode(kvMsg);
//        Protocol p = new Protocol();
        protocol.writeRequest(output, nextId, msgBytes);
        idToResponse.put(nextId, null);
        nextId += 1;
//        Returns:
//        the value, which is indexed by the given key.
//        Throws:
//        Exception – if put command cannot be executed (e.g. not connected to any KV server).
//        ???


        return null;
    }

    public void addListener(ClientSocketListener listener) {
        listeners.add(listener);
    }

    public synchronized void closeConnection() {
        logger.info("try to close connection ...");

        try {
            tearDownConnection();
            for (ClientSocketListener listener : listeners) {
                listener.handleStatus(
                        ClientSocketListener.SocketStatus.DISCONNECTED);
            }
        } catch (IOException ioe) {
            logger.error("Unable to close connection!");
        }
    }

    public void sendMessage(TextMessage msg) throws IOException {
        byte[] msgBytes = msg.getMsgBytes();
        output.write(msgBytes, 0, msgBytes.length);
        output.flush();
        logger.info("Send message:\t '" + msg.getMsg() + "'");
    }
}
