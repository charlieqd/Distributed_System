package server;

import org.apache.log4j.Logger;
import shared.IProtocol;
import shared.ISerializer;
import shared.Request;
import shared.Response;
import shared.messages.KVMessage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;


/**
 * Represents a connection end point for a particular client that is connected
 * to the server. This class is responsible for message reception and sending.
 * The class also implements the echo functionality. Thus whenever a message is
 * received it is going to be echoed back to the client.
 */
public class ClientConnection implements Runnable {

    private static Logger logger = Logger.getRootLogger();

    private boolean isOpen;
    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 128 * BUFFER_SIZE;

    private Socket clientSocket;
    private InputStream input;
    private OutputStream output;
    private final IProtocol protocol;
    private final ISerializer<KVMessage> messageSerializer;

    /**
     * Constructs a new CientConnection object for a given TCP socket.
     *
     * @param clientSocket the Socket object for the client connection.
     */
    public ClientConnection(Socket clientSocket,
                            IProtocol protocol,
                            ISerializer<KVMessage> messageSerializer) {
        this.clientSocket = clientSocket;
        this.isOpen = true;
        this.protocol = protocol;
        this.messageSerializer = messageSerializer;
    }

    /**
     * Initializes and starts the client connection. Loops until the connection
     * is closed or aborted by the client.
     */
    public void run() {
        try {
            output = clientSocket.getOutputStream();
            input = clientSocket.getInputStream();

            while (isOpen) {
                try {
                    Request request = receiveRequest();
                    if (request == null) {
                        continue;
                    }

                    KVMessage requestMessage = readMessage(request);
                    if (requestMessage == null) {
                        sendBadRequestResponse(request);
                        continue;
                    }

                    KVMessage responseMessage = null; // TODO

                    sendResponse(request, Response.Status.OK, responseMessage);

                } catch (IOException ioe) {
                    /* connection either terminated by the client or lost due to
                     * network problems */
                    logger.error("Error! Connection lost!", ioe);
                    isOpen = false;
                }
            }

        } catch (IOException ioe) {
            logger.error("Error! Connection could not be established!", ioe);

        } finally {

            try {
                if (clientSocket != null) {
                    input.close();
                    output.close();
                    clientSocket.close();
                }
            } catch (IOException ioe) {
                logger.error("Error! Unable to tear down connection!", ioe);
            }
        }
    }

    /**
     * Method sends a KVMessage using this socket.
     *
     * @param request the client request this response corresponds to.
     * @param message the message that is to be sent.
     * @throws IOException some I/O error regarding the output stream
     */
    public void sendResponse(Request request,
                             Response.Status status,
                             KVMessage message) throws IOException {
        byte[] messageBytes = null;
        if (message != null) {
            try {
                messageBytes = messageSerializer.encode(message);
            } catch (Exception e) {
                logger.error("Message serialization failed");
                throw e;
            }
        }

        byte[] responseBytes;
        try {
            responseBytes = protocol.encodeResponse(request, status,
                    messageBytes);
        } catch (Exception e) {
            logger.error("Response serialization failed");
            throw e;
        }

        output.write(responseBytes, 0, responseBytes.length);
        output.flush();
        logger.info("SEND \t<"
                + clientSocket.getInetAddress().getHostAddress() + ":"
                + clientSocket.getPort() + ">");
    }


    private Request receiveRequest() throws IOException {

        int index = 0;
        byte[] requestBytes = null, tmp = null;
        byte[] bufferBytes = new byte[BUFFER_SIZE];

        /* read first char from stream */
        byte read = (byte) input.read();
        boolean reading = true;

//		logger.info("First Char: " + read);
//		Check if stream is closed (read returns -1)
//		if (read == -1){
//			TextMessage msg = new TextMessage("");
//			return msg;
//		}

        while (/*read != 13  && */ read != 10 && read != -1 && reading) {/* CR, LF, error */
            /* if buffer filled, copy to msg array */
            if (index == BUFFER_SIZE) {
                if (requestBytes == null) {
                    tmp = new byte[BUFFER_SIZE];
                    System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
                } else {
                    tmp = new byte[requestBytes.length + BUFFER_SIZE];
                    System.arraycopy(requestBytes, 0, tmp, 0,
                            requestBytes.length);
                    System.arraycopy(bufferBytes, 0, tmp, requestBytes.length,
                            BUFFER_SIZE);
                }

                requestBytes = tmp;
                bufferBytes = new byte[BUFFER_SIZE];
                index = 0;
            }

            /* only read valid characters, i.e. letters and constants */
            bufferBytes[index] = read;
            index++;

            /* stop reading is DROP_SIZE is reached */
            if (requestBytes != null && requestBytes.length + index >= DROP_SIZE) {
                reading = false;
            }

            /* read next char from stream */
            read = (byte) input.read();
        }

        if (requestBytes == null) {
            tmp = new byte[index];
            System.arraycopy(bufferBytes, 0, tmp, 0, index);
        } else {
            tmp = new byte[requestBytes.length + index];
            System.arraycopy(requestBytes, 0, tmp, 0, requestBytes.length);
            System.arraycopy(bufferBytes, 0, tmp, requestBytes.length, index);
        }

        requestBytes = tmp;

        Request request;

        try {
            request = protocol.decodeRequest(requestBytes);
        } catch (Exception e) {
            logger.warn("Failed to parse request from \t<"
                    + clientSocket.getInetAddress().getHostAddress() + ":"
                    + clientSocket.getPort() + ">");
            return null;
        }

        logger.info("RECEIVE \t<"
                + clientSocket.getInetAddress().getHostAddress() + ":"
                + clientSocket.getPort() + ">");

        return request;
    }

    private KVMessage readMessage(Request request) {
        KVMessage message = null;
        try {
            message = messageSerializer.decode(request.getBody());
        } catch (Exception e) {
            logger.warn("Failed to parse message from \t<"
                    + clientSocket.getInetAddress().getHostAddress() + ":"
                    + clientSocket.getPort() + ">");
            return null;
        }

        return message;
    }

    private void sendBadRequestResponse(Request request) throws IOException {
        this.sendResponse(request, Response.Status.BAD_REQUEST, null);
    }

}
