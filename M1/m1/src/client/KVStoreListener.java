package client;

public interface KVStoreListener {

    public enum SocketStatus {CONNECTED, DISCONNECTED}

    /**
     * This method will be called when the socket status changes, such as
     * disconnected. WARNING: This method may be called from another thread.
     * Also, DO NOT call KVStore methods in this function.
     *
     * @param status the new status of the socket.
     */
    public void handleStatusChange(SocketStatus status);
}
