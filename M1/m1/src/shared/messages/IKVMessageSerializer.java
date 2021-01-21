package shared.messages;

import java.io.IOException;

public interface IKVMessageSerializer {
    KVMessage decode(byte[] bytes) throws IOException, ClassNotFoundException;

    byte[] encode(KVMessage message) throws IOException;
}
