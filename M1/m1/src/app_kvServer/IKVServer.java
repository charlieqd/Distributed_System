package app_kvServer;

public interface IKVServer {
    public enum CacheStrategy {
        None,
        LRU,
        LFU,
        FIFO;

        public static CacheStrategy fromString(String strategy) {
            switch (strategy) {
                case "LRU":
                    return CacheStrategy.LRU;
                case "FIFO":
                    return CacheStrategy.FIFO;
                case "None":
                    return CacheStrategy.None;
                default:
                    throw new IllegalArgumentException(
                            "Illegal cache strategy");
            }
        }
    }

    /**
     * Get the port number of the server
     *
     * @return port number
     */
    public int getPort();
}
