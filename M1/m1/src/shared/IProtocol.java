package shared;

public interface IProtocol {
    // For server

    Request decodeRequest(byte[] data);

    byte[] encodeResponse(Request request, byte[] encodedMessage);

    // For client

    byte[] encodeRequest(int id, byte[] encodedMessage);

    Response decodeResponse(byte[] data);
}
