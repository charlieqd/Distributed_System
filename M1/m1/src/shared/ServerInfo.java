package shared;

public class ServerInfo implements Comparable {
    public String ip;
    public int port;

    public String position;

    public ServerInfo(String ip, int port, String position) {
        this.ip = ip;
        this.port = port;
        this.position = position;
    }

    @Override
    public int compareTo(Object o) {
        return this.position.compareTo(((ServerInfo) o).position);
    }
}
