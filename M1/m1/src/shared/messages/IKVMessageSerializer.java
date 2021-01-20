package shared.messages;

public interface IKVMessageSerializer {
    KVMessage decode(byte[] bytes);

    byte[] encode(KVMessage message);
}
