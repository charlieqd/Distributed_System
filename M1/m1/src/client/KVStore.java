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

public class KVStore implements KVCommInterface {
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
    private Hashtable<Integer, Response> idToResponse;

    private KVMessageSerializer serializer;
    private Protocol protocol;


    public KVStore(String address, int port) throws IOException {
        // TODO Auto-generated method stub
        serverAddress = address;
        serverPort = port;
        nextId = 0;
        idToResponse = new Hashtable<Integer, Response>();
        listeners = new HashSet<ClientSocketListener>();
        serializer = new KVMessageSerializer();
        protocol = new Protocol();
    }

    private void setRunning(boolean run) {
        running = run;
    }

//    public void run() {
//        try {
//            output = clientSocket.getOutputStream();
//            input = clientSocket.getInputStream();
//
//            while (isRunning()) {
//                try {
//                    Response res = protocol.readResponse(input);
//                    for (ClientSocketListener listener : listeners) {
//                        listener.handleNewMessage(latestMsg);
//                    }
//                } catch (IOException ioe) {
//                    if (isRunning()) {
//                        logger.error("Connection lost!");
//                        try {
//                            tearDownConnection();
//                            for (ClientSocketListener listener : listeners) {
//                                listener.handleStatus(
//                                        ClientSocketListener.SocketStatus.CONNECTION_LOST);
//                            }
//                        } catch (IOException e) {
//                            logger.error("Unable to close connection!");
//                        }
//                    }
//                }
//            }
//        } catch (IOException ioe) {
//            logger.error("Connection could not be established!");
//
//        } finally {
//            if (isRunning()) {
//                disconnect();
//            }
//        }
//    }

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

    private KVMessage receiveMessage() throws Exception {
        try {
            Response res = protocol.readResponse(input);
            int status = res.getStatus();
            if (status == Response.Status.OK) {
                byte[] msgByte = res.getBody();
                int id = res.getId();
                idToResponse.put(id, res);
                KVMessage latestMsg = serializer.decode(msgByte);
                for (ClientSocketListener listener : listeners) {
                    listener.handleNewMessage(latestMsg);
                }
                return latestMsg;
            } else {
                throw new Exception("Incorrect Status!");
            }
        } catch (IOException ioe) {
            if (isRunning()) {
                logger.error(
                        "Command cannot be executed! Connection lost!");
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
            throw new Exception("Command cannot be executed!");
        }
    }

    private void sendRequest(String key, String value,
                             KVMessage.StatusType status) throws
            IOException {
        try {
            KVMessage kvMsg = new KVMessageImpl(key, value, status);
            byte[] msgBytes = serializer.encode(kvMsg);
            protocol.writeRequest(output, nextId, msgBytes);
            idToResponse.put(nextId, null);
            nextId += 1;
        } catch (IOException e) {
            logger.error("Unable to send message!");
            disconnect();
        }
    }


    @Override
    public void connect() throws Exception {
        // TODO Auto-generated method stub
        clientSocket = new Socket(serverAddress, serverPort);
        output = clientSocket.getOutputStream();
        input = clientSocket.getInputStream();
        setRunning(true);

        try {
            Response res = protocol.readResponse(input);
            int status = res.getStatus();
            if (status == Response.Status.CONNECTION_ESTABLISHED) {
                logger.info("Connection Established!");
            } else {
                logger.error("Connection Error!");
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

    @Override
    public void disconnect() {
        // TODO Auto-generated method stub
        logger.info("try to close connection ...");

        try {
            tearDownConnection();
            try {
                for (ClientSocketListener listener : listeners) {
                    listener.handleStatus(
                            ClientSocketListener.SocketStatus.DISCONNECTED);
                }
            } catch (Exception e) {
                logger.error("No connection to close!");
            }

        } catch (IOException ioe) {
            logger.error("Unable to close connection!");
        }
    }

    @Override
    public KVMessage put(String key, String value) throws Exception {
        // TODO Auto-generated method stub
        sendRequest(key, value, KVMessage.StatusType.PUT);
        /*Returns: a message that confirms the insertion of the tuple or an error.
        Throws: Exception – if put command cannot be executed (e.g. not connected to any KV server).*/
        return receiveMessage();
    }

    @Override
    public KVMessage get(String key) throws Exception {
        // TODO Auto-generated method stub
        sendRequest(key, null, KVMessage.StatusType.GET);
        /*Returns: the value, which is indexed by the given key.
        Throws: Exception – if get command cannot be executed (e.g. not connected to any KV server).*/
        return receiveMessage();
    }

    public void addListener(ClientSocketListener listener) {
        listeners.add(listener);
    }


    public void sendMessage(TextMessage msg) throws IOException {
        byte[] msgBytes = msg.getMsgBytes();
        output.write(msgBytes, 0, msgBytes.length);
        output.flush();
        logger.info("Send message:\t '" + msg.getMsg() + "'");
    }
}
