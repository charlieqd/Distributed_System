package shared;

import java.io.Serializable;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

public class Metadata implements Serializable {

    private static final long serialVersionUID = 2654489690409479031L;

    private static MessageDigest hashGenerator;

    static {
        try {
            hashGenerator = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new Error(Util.getStackTraceString(e));
        }
    }

    // sorted
    private ArrayList<ServerInfo> servers;

    public Metadata(ArrayList<ServerInfo> servers) {
        this.servers = servers;
    }

    public List<ServerInfo> getServers() {
        return servers;
    }

    public ServerInfo getServer(String ringPosition) {
        if (servers.size() == 0) {
            return null;
        }
        return binarySearch(ringPosition);
    }

    public ServerInfo binarySearch(String ringPosition) {
        int left = 0;
        int right = servers.size() - 1;
        while (left < right) {
            int mid = (left + right) / 2;
            if (ringPosition.compareTo(servers.get(mid).position) < 0) {
                // ringPosition < mid
                right = mid;
            } else if (ringPosition.compareTo(servers.get(mid).position) == 0) {
                // ringPosition == mid
                return servers.get(mid);
            } else {
                // ringPosition > mid
                left = left + 1;
            }
        }

        if (left == 0) {
            return servers.get(0);
        }

        if (left == servers.size() - 1 && ringPosition
                .compareTo(servers.get(left).position) > 0) {
            return servers.get(0);
        }

        return servers.get(left);


    }

    public static String getRingPosition(String key) {
        byte[] bytes = hashGenerator.digest(key.getBytes());
        return String.format("%032x", new BigInteger(1, bytes));
    }

}
