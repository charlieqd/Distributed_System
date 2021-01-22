package shared;

public class Response {

    public static class Status {
        public static final int OK = 0;
        public static final int CONNECTION_ESTABLISHED = 1;
        public static final int BAD_REQUEST = 2;
    }

    private byte[] body;
    private int id;
    private int status;

    public Response(byte[] body, int id, int status) {
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

    public int getStatus() {
        return status;
    }
}
