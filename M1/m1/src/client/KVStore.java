package client;

import org.apache.log4j.Logger;
import shared.messages.KVMessage;
import shared.messages.TextMessage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.HashSet;
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


    public KVStore(String address, int port) throws IOException {
        // TODO Auto-generated method stub
        clientSocket = new Socket(address, port);
        listeners = new HashSet<ClientSocketListener>();
        setRunning(true);
        logger.info("Connection established");
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
        
        return null;
    }

    @Override
    public KVMessage get(String key) throws Exception {
        // TODO Auto-generated method stub
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
