package server;

/**
 * NOTE: Methods must be thread-safe.
 */
public interface IKVStorage {
    String get(String key);

    void put(String key, String value);
}
