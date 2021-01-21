package shared;

public interface IProtocol {
    public byte[] encode(byte[] message);

    public ProtocolData decode(byte[] data);
}
