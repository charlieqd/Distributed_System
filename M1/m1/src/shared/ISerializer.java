package shared;

import java.io.IOException;

public interface ISerializer<T> {
    T decode(byte[] bytes) throws IOException, ClassNotFoundException;

    byte[] encode(T message) throws IOException;
}
