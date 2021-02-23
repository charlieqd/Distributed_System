package ecs;

import java.io.Serializable;

public class MoveDataArgs implements Serializable {
    private static final long serialVersionUID = 8056847332433416586L;

    private String hashRangeStart;
    private String hashRangeEnd;
    private String address;
    private int port;

    public MoveDataArgs(String hashRangeStart,
                        String hashRangeEnd,
                        String address, int port) {
        this.hashRangeStart = hashRangeStart;
        this.hashRangeEnd = hashRangeEnd;
        this.address = address;
        this.port = port;
    }

    public String getHashRangeStart() {
        return hashRangeStart;
    }

    public String getHashRangeEnd() {
        return hashRangeEnd;
    }

    public String getAddress() {
        return address;
    }

    public int getPort() {
        return port;
    }

    @Override
    public String toString() {
        return "MoveDataArgs{" +
                "hashRangeStart='" + hashRangeStart + '\'' +
                ", hashRangeEnd='" + hashRangeEnd + '\'' +
                ", address='" + address + '\'' +
                ", port=" + port +
                '}';
    }
}
