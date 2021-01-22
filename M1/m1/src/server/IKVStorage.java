package server;

import java.io.IOException;

/**
 * NOTE: Methods must be thread-safe.
 */
public interface IKVStorage {
    String get(String key) throws IOException;

    void put(String key, String value) throws IOException;
}
