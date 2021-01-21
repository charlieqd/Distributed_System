package shared.messages;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

public class KVMessageSerializer implements IKVMessageSerializer {
    @Override
    public KVMessage decode(byte[] bytes) {
        return null;
    }

    @Override
    public byte[] encode(KVMessage message) throws IOException {
        try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
            try (ObjectOutputStream objectStream = new ObjectOutputStream(
                    stream)) {
                objectStream.writeObject(message);
            }
            return stream.toByteArray();
        }
    }
}
