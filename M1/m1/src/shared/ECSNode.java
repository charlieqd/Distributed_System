package shared;

import shared.messages.IECSNode;

import java.io.Serializable;

public class ECSNode implements Comparable, IECSNode, Serializable {

    private static final long serialVersionUID = 2917446459158862564L;

    private String host;
    private int port;
    private String name;
    private String position;

    public ECSNode(String host, int port, String position) {
        this.host = host;
        this.port = port;
        this.position = position;
    }

    @Override
    public int compareTo(Object o) {
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

}
