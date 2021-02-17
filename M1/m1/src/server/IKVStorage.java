package server;

import shared.messages.KVMessage;

import java.io.IOException;
import java.util.List;

/**
 * NOTE: Methods must be thread-safe.
 */
public interface IKVStorage {
    String get(String key) throws IOException;

    KVMessage.StatusType put(String key, String value) throws IOException;

    void clearCache();

    List<String> getAllKeys(String hashRangeStart,
                            String hashRangeEnd) throws IOException;
}
