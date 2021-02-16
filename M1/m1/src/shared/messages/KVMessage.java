package shared.messages;

import shared.Metadata;

public interface KVMessage {

    public enum StatusType {
        CONNECTED,          /* Received when first connected */
        DISCONNECT,         /* Ask server to disconnect */
        GET,                /* Get - request */
        GET_ERROR,          /* requested tuple (i.e. value) not found */
        GET_SUCCESS,        /* requested tuple (i.e. value) found */
        PUT,                /* Put - request */
        PUT_SUCCESS,        /* Put - request successful, tuple inserted */
        PUT_UPDATE,         /* Put - request successful, i.e. value updated */
        PUT_ERROR,          /* Put - request not successful */
        DELETE_SUCCESS,     /* Delete - request successful */
        DELETE_ERROR,       /* Delete - request successful */
        NOT_RESPONSIBLE,    /* server not responsible for the key */
        SERVER_WRITE_LOCK,  /* server has locked write operations */
        SERVER_STOPPED,     /* server is stopped */
        ECS_START_SERVING,  /* ECS: order this server to allow all client requests */
        ECS_STOP_SERVING,   /* ECS: order this server to block all client requests */
        ECS_SHUT_DOWN,      /* ECS: order this server to shut down */
        ECS_LOCK_WRITE,     /* ECS: order this server to lock write operations */
        ECS_UNLOCK_WRITE,   /* ECS: order this server to unlock write operations */
        ECS_MOVE_DATA,      /* ECS: order this server to transfer data */
        ECS_UPDATE_METADATA,/* ECS: update metadata cache */
        FAILED              /* Any other error */
    }

    /**
     * @return the key that is associated with this message, null if no key is
     * associated.
     */
    public String getKey();

    /**
     * @return the value that is associated with this message, null if no value
     * is associated.
     */
    public String getValue();

    /**
     * @return the metadata associated with this message (e.g. for
     * NOT_RESPONSIBLE status), null if no value is associated.
     */
    public Metadata getMetadata();

    /**
     * @return a status string that is used to identify request types, response
     * types and error types associated to the message.
     */
    public StatusType getStatus();

}


