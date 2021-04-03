package shared.messages;

import shared.Metadata;

public interface KVMessage {

    public enum StatusType {
        /**
         * Received when first connected
         */
        CONNECTED,
        /**
         * Ask server to disconnect
         */
        DISCONNECT,
        /**
         * Get - request
         */
        GET,
        /**
         * requested tuple (i.e. value) not found
         */
        GET_ERROR,
        /**
         * requested tuple (i.e. value) found
         */
        GET_SUCCESS,
        /**
         * Put - request
         */
        PUT,
        /**
         * Put - request successful, tuple inserted
         */
        PUT_SUCCESS,
        /**
         * Put - request successful, i.e. value updated
         */
        PUT_UPDATE,
        /**
         * Put - request not successful
         */
        PUT_ERROR,
        /**
         * Delete - request successful
         */
        DELETE_SUCCESS,
        /**
         * Delete - request successful
         */
        DELETE_ERROR,
        /**
         * server not responsible for the key
         */
        NOT_RESPONSIBLE,
        /**
         * server has locked write operations
         */
        SERVER_WRITE_LOCK,
        /**
         * server is stopped
         */
        SERVER_STOPPED,
        /**
         * A special PUT request used during data transfer, to avoid serving
         * lock and write lock
         */
        ECS_PUT,
        /**
         * ECS: signal to ECS that the last command was successful
         */
        ECS_SUCCESS,
        /**
         * ECS: order this server to allow all client requests
         */
        ECS_START_SERVING,
        /**
         * ECS: order this server to block all client requests
         */
        ECS_STOP_SERVING,
        /**
         * ECS: order this server to shutdown
         */
        ECS_SHUTDOWN,
        /**
         * ECS: order this server to lock write operations
         */
        ECS_LOCK_WRITE,
        /**
         * ECS: order this server to unlock write operations
         */
        ECS_UNLOCK_WRITE,
        /**
         * ECS: order this server to transfer data
         */
        ECS_COPY_DATA,
        /**
         * ECS: order this server to delete data
         */
        ECS_DELETE_DATA,
        /**
         * ECS: update metadata cache
         */
        ECS_UPDATE_METADATA,
        /**
         * ECS: start replicator
         */
        ECS_START_REPLICATION,
        /**
         * ECS: stop replicator
         */
        ECS_STOP_REPLICATION,
        /**
         * Transaction: Start Transaction
         */
        TRANSACTION_BEGIN,
        /**
         * Transaction: Operation Success
         */
        TRANSACTION_SUCCESS,
        /**
         * Transaction: commit transaction
         */
        TRANSACTION_COMMIT,
        /**
         * Transaction: rollback transaction
         */
        TRANSACTION_ROLLBACK,
        /**
         * Transaction: get
         */
        TRANSACTION_GET,
        /**
         * Transaction: put
         */
        TRANSACTION_PUT,

        /**
         * Any other error
         */
        FAILED
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
     * @return the arguments for ECS commands (e.g. ECS_COPY_DATA); null if this
     * message is not an ECS message or if the ECS message has no argument (e.g.
     * ECS_START_SERVING).
     */
    public Object getECSCommandArg();

    /**
     * @return a status string that is used to identify request types, response
     * types and error types associated to the message.
     */
    public StatusType getStatus();

}
