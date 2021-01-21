package shared;

public class ProtocolData {
    
    private byte[] message;
    private int id;

    public ProtocolData(byte[] message, int id) {
        this.message = message;
        this.id = id;
    }
}
