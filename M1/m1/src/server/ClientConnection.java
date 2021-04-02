package server;

import app_kvServer.KVServer;
import client.ServerConnection;
import ecs.MoveDataArgs;
import org.apache.log4j.Logger;
import shared.*;
import shared.messages.KVMessage;
import shared.messages.KVMessageImpl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;


/**
 * Represents a connection end point for a particular client that is connected
 * to the server.
 */
public class ClientConnection implements Runnable {

    private static final Logger logger = Logger.getRootLogger();

    private boolean isOpen;

    private final Socket clientSocket;
    private InputStream input;
    private OutputStream output;
    private final IKVStorage storage;
    private final IProtocol protocol;
    private final ISerializer<KVMessage> messageSerializer;
    private boolean inTransaction;
    private KVStorageDelta transactionBuffer = null;

    private KVServer server;

    /**
     * Constructs a new CientConnection object for a given TCP socket.
     *
     * @param clientSocket the Socket object for the client connection.
     */
    public ClientConnection(KVServer server,
                            Socket clientSocket,
                            IKVStorage storage,
                            IProtocol protocol,
                            ISerializer<KVMessage> messageSerializer) {
        this.server = server;
        this.clientSocket = clientSocket;
        this.isOpen = true;
        this.storage = storage;
        this.protocol = protocol;
        this.messageSerializer = messageSerializer;
        this.inTransaction = false;
    }

    /**
     * Initializes and starts the client connection. Loops until the connection
     * is closed or aborted by the client.
     */
    public void run() {
        try {
            server.registerClientConnection(this);

            output = clientSocket.getOutputStream();
            input = clientSocket.getInputStream();

            sendResponse(output, null, Response.Status.CONNECTION_ESTABLISHED,
                    new KVMessageImpl(null, null, server.metadata.get(),
                            KVMessage.StatusType.CONNECTED));

            while (isOpen && server.isRunning()) {
                try {
                    Request request = receiveRequest(input);
                    if (request == null) {
                        continue;
                    }

                    KVMessage requestMessage = readMessage(request);
                    if (requestMessage == null) {
                        logger.debug("RECEIVE \t<"
                                + clientSocket.getInetAddress()
                                .getHostAddress() + ":"
                                + clientSocket
                                .getPort() + ">: invalid message");

                        sendResponse(output, request, Response.Status.OK,
                                new KVMessageImpl(null, "Invalid message",
                                        KVMessage.StatusType.FAILED));
                        continue;
                    }

                    logger.debug("RECEIVE \t<"
                            + clientSocket.getInetAddress()
                            .getHostAddress() + ":"
                            + clientSocket.getPort() + ">: '"
                            + requestMessage.toString() + "'");

                    handleMessage(output, request, requestMessage);

                } catch (IOException ioe) {
                    /* connection either terminated by the client or lost due to
                     * network problems */
                    logger.error(
                            "Error: Failed to read socket input. Closing connection.",
                            ioe);
                    isOpen = false;
                }
            }

        } catch (IOException ioe) {
            logger.error("Error: Connection could not be established.", ioe);

        } finally {
            server.unregisterClientConnection(this);
            try {
                if (clientSocket != null) {
                    input.close();
                    output.close();
                    clientSocket.close();
                }
            } catch (IOException ioe) {
                logger.error("Error: Unable to tear down connection.", ioe);
            }
        }
    }

    public void disconnect() {
        try {
            clientSocket.close();
        } catch (IOException e) {
            logger.error("Failed to close client socket", e);
        }
    }

    private KVMessage handleNotServing() {
        return new KVMessageImpl(null, null,
                KVMessage.StatusType.SERVER_STOPPED);
    }

    private KVMessage handleWriteLocked() {
        return new KVMessageImpl(null, null,
                KVMessage.StatusType.SERVER_WRITE_LOCK);
    }

    private boolean isResponsibleForKeyWrite(String key) {
        String nodeName = server.getNodeName();
        if (nodeName == null) {
            logger.error("Server name is null");
            return false;
        }
        Metadata metadata = server.metadata.get();
        if (metadata == null) return false;

        ECSNode node = metadata.getServer(Metadata.getRingPosition(key));
        return node != null && nodeName.equals(node.getNodeName());
    }

    private boolean isResponsibleForKeyRead(String key) {
        String nodeName = server.getNodeName();
        if (nodeName == null) {
            logger.error("Server name is null");
            return false;
        }
        Metadata metadata = server.metadata.get();
        if (metadata == null) return false;

        ECSNode node = metadata.getServer(Metadata.getRingPosition(key));
        if (node == null) {
            return false;
        }
        if (nodeName.equals(node.getNodeName())) {
            return true;
        }
        // Replicas can serve read request
        node = metadata.getSuccessor(node);
        if (nodeName.equals(node.getNodeName())) {
            return true;
        }
        node = metadata.getSuccessor(node);
        if (nodeName.equals(node.getNodeName())) {
            return true;
        }
        return false;
    }

    private KVMessage handleNotResponsible() {
        return new KVMessageImpl(null, null, server.metadata.get(),
                KVMessage.StatusType.NOT_RESPONSIBLE);
    }

    private void handleMessage(OutputStream output,
                               Request request,
                               KVMessage requestMessage) throws IOException {

        KVMessage responseMessage = null;

        KVMessage.StatusType requestStatus = requestMessage.getStatus();

        boolean shouldShutdown = false;

        switch (requestStatus) {
            case DISCONNECT: {
                isOpen = false;
                logger.info("Client disconnected \t<"
                        + clientSocket.getInetAddress()
                        .getHostAddress() + ":"
                        + clientSocket.getPort() + ">");
                return;
            }

            case GET: {
                String key = requestMessage.getKey();
                if (key == null) {
                    responseMessage = new KVMessageImpl(null, "Invalid key",
                            KVMessage.StatusType.FAILED);
                    break;
                }
                if (!server.serving.get()) {
                    responseMessage = handleNotServing();
                } else if (!isResponsibleForKeyRead(key)) {
                    responseMessage = handleNotResponsible();
                } else {
                    String value;
                    if(inTransaction){
                        value = transactionBuffer.
                    }
                    try {
                        value = storage.get(key);
                    } catch (IOException e) {
                        responseMessage = new KVMessageImpl(null,
                                "Internal server error: " +
                                        Util.getStackTraceString(e),
                                KVMessage.StatusType.FAILED);
                        break;
                    }
                    if (value == null) {
                        responseMessage = new KVMessageImpl(key, null,
                                KVMessage.StatusType.GET_ERROR);
                    } else {
                        responseMessage = new KVMessageImpl(key, value,
                                KVMessage.StatusType.GET_SUCCESS);
                    }
                    break;
                }
                break;
            }

            case ECS_PUT:
            case PUT: {
                String key = requestMessage.getKey();
                if (key == null) {
                    responseMessage = new KVMessageImpl(null, "Invalid key",
                            KVMessage.StatusType.FAILED);
                    break;
                }

                // An ECS_PUT will bypass all checks
                boolean isClientPut = requestStatus == KVMessage.StatusType.PUT;

                if (isClientPut && !server.serving.get()) {
                    responseMessage = handleNotServing();
                } else if (isClientPut && !isResponsibleForKeyWrite(key)) {
                    responseMessage = handleNotResponsible();
                } else if (isClientPut && server.writeLock.get()) {
                    responseMessage = handleWriteLocked();
                } else if (isClientPut && server.selfWriteLock.get()) {
                    responseMessage = handleWriteLocked();
                } else {
                    String value = requestMessage.getValue();

                    KVMessage.StatusType putResponseType;
                    if(!inTransaction){
                        try {
                            putResponseType = storage.put(key, value);
                        } catch (IOException e) {
                            responseMessage = new KVMessageImpl(null,
                                    "Internal server error: " +
                                            Util.getStackTraceString(e),
                                    KVMessage.StatusType.FAILED);
                            break;
                        }
                    }
                    else{
                        if(this.server.isKeyLock(key)){
                            putResponseType = KVMessage.StatusType.SERVER_WRITE_LOCK;
                        }else{
                            transactionBuffer.put(key, value);
                            putResponseType = KVMessage.StatusType.TRANSACTION_SUCCESS;
                            // add key to the server lock key hashset
                        }
                    }

                    responseMessage = new KVMessageImpl(key, value,
                            putResponseType);
                }
                break;
            }

            case ECS_START_SERVING: {
                try {
                    server.startServing();
                    responseMessage = new KVMessageImpl(null, null,
                            KVMessage.StatusType.ECS_SUCCESS);
                } catch (Exception e) {
                    responseMessage = new KVMessageImpl(null,
                            "Internal server error: " + Util
                                    .getStackTraceString(e),
                            KVMessage.StatusType.FAILED);
                }
                break;
            }

            case ECS_STOP_SERVING: {
                try {
                    server.stopServing();
                    responseMessage = new KVMessageImpl(null, null,
                            KVMessage.StatusType.ECS_SUCCESS);
                } catch (Exception e) {
                    responseMessage = new KVMessageImpl(null,
                            "Internal server error: " + Util
                                    .getStackTraceString(e),
                            KVMessage.StatusType.FAILED);
                }
                break;
            }

            case ECS_SHUTDOWN: {
                // We first respond to the client, then shutdown the server.
                shouldShutdown = true;
                responseMessage = new KVMessageImpl(null, null,
                        KVMessage.StatusType.ECS_SUCCESS);
                break;
            }

            case ECS_LOCK_WRITE: {
                try {
                    server.lockWrite();
                    responseMessage = new KVMessageImpl(null, null,
                            KVMessage.StatusType.ECS_SUCCESS);
                } catch (Exception e) {
                    responseMessage = new KVMessageImpl(null,
                            "Internal server error: " + Util
                                    .getStackTraceString(e),
                            KVMessage.StatusType.FAILED);
                }
                break;
            }

            case ECS_UNLOCK_WRITE: {
                try {
                    server.unlockWrite();
                    responseMessage = new KVMessageImpl(null, null,
                            KVMessage.StatusType.ECS_SUCCESS);
                } catch (Exception e) {
                    responseMessage = new KVMessageImpl(null,
                            "Internal server error: " + Util
                                    .getStackTraceString(e),
                            KVMessage.StatusType.FAILED);
                }
                break;
            }

            case ECS_COPY_DATA: {
                try {
                    MoveDataArgs arg = (MoveDataArgs) requestMessage
                            .getECSCommandArg();
                    boolean success = server.copyDataTo(arg.getHashRangeStart(),
                            arg.getHashRangeEnd(), arg.getAddress(),
                            arg.getPort());
                    if (success) {
                        responseMessage = new KVMessageImpl(null, null,
                                KVMessage.StatusType.ECS_SUCCESS);
                    } else {
                        responseMessage = new KVMessageImpl(null,
                                "Failed to copy data",
                                KVMessage.StatusType.FAILED);
                    }
                } catch (Exception e) {
                    responseMessage = new KVMessageImpl(null,
                            "Internal server error: " + Util
                                    .getStackTraceString(e),
                            KVMessage.StatusType.FAILED);
                }
                break;
            }

            case ECS_DELETE_DATA: {
                try {
                    MoveDataArgs arg = (MoveDataArgs) requestMessage
                            .getECSCommandArg();
                    boolean success = server.deleteData(arg.getHashRangeStart(),
                            arg.getHashRangeEnd());
                    if (success) {
                        responseMessage = new KVMessageImpl(null, null,
                                KVMessage.StatusType.ECS_SUCCESS);
                    } else {
                        responseMessage = new KVMessageImpl(null,
                                "Failed to delete data",
                                KVMessage.StatusType.FAILED);
                    }
                } catch (Exception e) {
                    responseMessage = new KVMessageImpl(null,
                            "Internal server error: " + Util
                                    .getStackTraceString(e),
                            KVMessage.StatusType.FAILED);
                }
                break;
            }

            case ECS_UPDATE_METADATA: {
                try {
                    server.updateMetadata(requestMessage.getMetadata());
                    responseMessage = new KVMessageImpl(null, null,
                            KVMessage.StatusType.ECS_SUCCESS);
                } catch (Exception e) {
                    responseMessage = new KVMessageImpl(null,
                            "Internal server error: " + Util
                                    .getStackTraceString(e),
                            KVMessage.StatusType.FAILED);
                }
                break;
            }

            case ECS_START_REPLICATION: {
                try {
                    server.getReplicator().startReplication();
                    responseMessage = new KVMessageImpl(null, null,
                            KVMessage.StatusType.ECS_SUCCESS);
                } catch (Exception e) {
                    responseMessage = new KVMessageImpl(null,
                            "Internal server error: " + Util
                                    .getStackTraceString(e),
                            KVMessage.StatusType.FAILED);
                }
                break;
            }

            case ECS_STOP_REPLICATION: {
                try {
                    server.getReplicator().stopReplication();
                    responseMessage = new KVMessageImpl(null, null,
                            KVMessage.StatusType.ECS_SUCCESS);
                } catch (Exception e) {
                    responseMessage = new KVMessageImpl(null,
                            "Internal server error: " + Util
                                    .getStackTraceString(e),
                            KVMessage.StatusType.FAILED);
                }
                break;
            }

            case TRANSACTION_BEGIN:{
                try {
                    this.inTransaction = true;
                    responseMessage = new KVMessageImpl(null, null,
                            KVMessage.StatusType.TRANSACTION_SUCCESS);
                } catch (Exception e) {
                    responseMessage = new KVMessageImpl(null,
                            "Transaction begin error: " + Util
                                    .getStackTraceString(e),
                            KVMessage.StatusType.FAILED);
                }
                break;
            }

            default: {
                responseMessage = new KVMessageImpl(null,
                        "Bad request status type: " +
                                requestMessage.getStatus().toString(),
                        KVMessage.StatusType.FAILED);
                break;
            }
        }

        if (responseMessage == null) {
            if (logger.isDebugEnabled()) {
                logger.debug("SEND \t<"
                        + clientSocket.getInetAddress()
                        .getHostAddress() + ":"
                        + clientSocket.getPort() + ">: null");
            }
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("SEND \t<"
                        + clientSocket.getInetAddress()
                        .getHostAddress() + ":"
                        + clientSocket.getPort() + ">: '"
                        + responseMessage.toString() + "'");
            }
        }

        sendResponse(output, request, Response.Status.OK,
                responseMessage);

        if (shouldShutdown) {
            try {
                server.shutDown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Method sends a KVMessage using this socket.
     *
     * @param output  the output stream to write response to.
     * @param request the client request this response corresponds to.
     * @param status  the status of the response.
     * @param message the message that is to be sent; can be null.
     * @throws IOException some I/O error regarding the output stream
     */
    public void sendResponse(OutputStream output,
                             Request request,
                             int status,
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

        protocol.writeResponse(output, request, status,
                messageBytes);
    }


    /**
     * @param input the input stream to read request from.
     * @return the request object.
     * @throws IOException when reading request from socket failed.
     */
    private Request receiveRequest(InputStream input) throws IOException {
        Request request = protocol.readRequest(input);

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

}
