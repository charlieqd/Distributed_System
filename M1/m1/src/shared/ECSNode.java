package shared;

import shared.messages.IECSNode;

import java.io.Serializable;

public class ECSNode implements Comparable<ECSNode>, IECSNode, Serializable {

    private static final long serialVersionUID = 2917446459158862564L;

    private String host;
    private int port;
    private String name;
    private String position;

    public ECSNode(String name, String host, int port, String position) {
        this.name = name;
        this.host = host;
        this.port = port;
        this.position = position;
    }

    public ECSNode(String name, String host, int port) {
        this(name, host, port, getRingPosition(host, port));
    }

    @Override
    public int compareTo(ECSNode o) {
        return this.position.compareTo(((ECSNode) o).position);
    }

    @Override
    public String getNodeName() {
        return name;
    }

    @Override
    public String getNodeHost() {
        return host;
    }

    @Override
    public int getNodePort() {
        return port;
    }

    public String getPosition() {
        return position;
    }

    @Override
    public String toString() {
        return "ECSNode{" +
                "host='" + host + '\'' +
                ", port=" + port +
                ", name='" + name + '\'' +
                ", position='" + position + '\'' +
                '}';
    }

    public static String getRingPosition(String host, int port) {
        return Metadata.getRingPosition(String.format("%s:%d", host, port));
    }

}
