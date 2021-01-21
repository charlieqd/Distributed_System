package shared.messages;

import java.io.*;

public class KVMessageSerializer implements IKVMessageSerializer {
    @Override
    public KVMessage decode(byte[] bytes) throws IOException,
            ClassNotFoundException {
        try (ByteArrayInputStream stream = new ByteArrayInputStream(bytes)) {
            try (ObjectInputStream objectStream = new ObjectInputStream(
                    stream)) {
                try {
                    return ((KVMessage) objectStream.readObject());
                } catch (ClassCastException exception) {
                    throw new ClassNotFoundException(
                            "Failed to cast to KVMessage");
                }
            }
        }
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
