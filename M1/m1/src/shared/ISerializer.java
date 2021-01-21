package shared;

import java.io.IOException;

/**
 * NOTE: Methods must be thread-safe.
 */
public interface ISerializer<T> {
    T decode(byte[] bytes) throws IOException, ClassNotFoundException;

    byte[] encode(T message) throws IOException;
}
