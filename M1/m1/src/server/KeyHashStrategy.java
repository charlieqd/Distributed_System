package server;

/**
 * NOTE: Methods must be thread-safe.
 */
public interface KeyHashStrategy {
    /**
     * Compute the hash value of a given key to determine which file this key is
     * stored in. This method must be thread-safe.
     */
    String hashKey(String key);
}
