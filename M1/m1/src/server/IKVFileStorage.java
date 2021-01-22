package server;

import shared.messages.KVMessage;

import java.io.IOException;

/**
 * NOTE: Methods may not be thread-safe.
 */
public interface IKVFileStorage {
    public String read(String key) throws IOException;

    public KVMessage.StatusType write(String key, String value) throws
            IOException;
}
