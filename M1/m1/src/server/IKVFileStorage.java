package server;

import java.io.IOException;

/**
 * NOTE: Methods may not be thread-safe.
 */
public interface IKVFileStorage {
    public String read(String key) throws IOException;

    public void write(String key, String value) throws IOException;
}
