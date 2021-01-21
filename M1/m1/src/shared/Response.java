package shared;

public class Response {

    public enum Status {
        OK,
        BAD_REQUEST
    }

    private byte[] body;
    private int id;
    private Status status;

    public Response(byte[] body, int id, Status status) {
        this.body = body;
        this.id = id;
        this.status = status;
    }

    public int getId() {
        return id;
    }

    public byte[] getBody() {
        return body;
    }

    public Status getStatus() {
        return status;
    }
}
