package ecs;

import client.ServerConnection;
import shared.ECSNode;
import shared.IProtocol;
import shared.ISerializer;
import shared.messages.KVMessage;

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
        LAUNCHED
    }

    private Status status = Status.NOT_LAUNCHED;

    private final ServerConnection connection;

    public ECSNodeState(IProtocol protocol,
                        ISerializer<KVMessage> serializer,
                        ECSNode node) {
        connection = new ServerConnection(protocol,
                serializer,
                node.getNodeHost(),
                node.getNodePort());
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public ServerConnection getConnection() {
        return connection;
    }

}
