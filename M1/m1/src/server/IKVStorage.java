package server;

import shared.messages.KVMessage;

import java.io.IOException;

/**
 * NOTE: Methods must be thread-safe.
 */
public interface IKVStorage {
    String get(String key) throws IOException;

    KVMessage.StatusType put(String key, String value) throws IOException;
}
