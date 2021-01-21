package shared;

public class Request {

    private byte[] message;
    private int id;

    public Request(byte[] message, int id) {
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
