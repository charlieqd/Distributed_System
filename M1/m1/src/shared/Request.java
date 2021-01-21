package shared;

public class Request {

    private byte[] body;
    private int id;

    public Request(byte[] body, int id) {
        this.body = body;
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public byte[] getBody() {
        return body;
    }
}
