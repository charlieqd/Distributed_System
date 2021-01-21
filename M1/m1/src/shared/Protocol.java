package shared;

// TODO: implement this
public class Protocol implements IProtocol {
    @Override
    public Request decodeRequest(byte[] data) {
        return null;
    }

    @Override
    public byte[] encodeResponse(Request request,
                                 Response.Status status,
                                 byte[] encodedMessage) {
        return new byte[0];
    }

    @Override
    public byte[] encodeRequest(int id, byte[] encodedMessage) {
        return new byte[0];
    }

    @Override
    public Response decodeResponse(byte[] data) {
        return null;
    }
}
