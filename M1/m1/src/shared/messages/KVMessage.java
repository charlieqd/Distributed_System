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


