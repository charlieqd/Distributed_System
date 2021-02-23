package ecs;

import client.ServerConnection;
import shared.ECSNode;
import shared.IProtocol;
import shared.ISerializer;
import shared.messages.KVMessage;

import java.util.concurrent.atomic.AtomicReference;

public class ECSNodeState {
    public enum Status {
        /**
         * The server has not been launched, or has been shutdown.
         */
        NOT_LAUNCHED,

        /**
         * The ECS has tried to launched the server, but a connection has not
         * been established.
         */
        LAUNCHING,

        /**
         * The server is successfully launched and a socket connection has been
         * established.
         */
        LAUNCHED,

        /**
         * The server is successfully launched and a socket connection has been
         * established. However, the server is not yet added to the list of
         * active servers, which means it will not receive requests yet.
         */
        CONNECTED,

        /**
         * The server is successfully setup and added to the list of active
         * servers.
         */
        ACTIVATED
    }

    private AtomicReference<Status> status = new AtomicReference<>(
            Status.NOT_LAUNCHED);

    private final ServerConnection connection;

    private ECSNode node;

    public ECSNodeState(IProtocol protocol,
                        ISerializer<KVMessage> serializer,
                        ECSNode node) {
        this.node = node;
        connection = new ServerConnection(protocol,
                serializer,
                node.getNodeHost(),
                node.getNodePort());
    }

    public AtomicReference<Status> getStatus() {
        return status;
    }

    public ECSNode getNode() {
        return node;
    }

    public ServerConnection getConnection() {
        return connection;
    }

    @Override
    public String toString() {
        return String.format("status: %s, connection valid = " + connection
                .isConnectionValid(), status.get().name());
    }
}
