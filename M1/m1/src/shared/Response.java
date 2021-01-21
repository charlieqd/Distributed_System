package shared;

public class Response {

    private byte[] message;
    private int id;

    public Response(byte[] message, int id) {
        this.message = message;
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public byte[] getMessage() {
        return message;
    }
}
