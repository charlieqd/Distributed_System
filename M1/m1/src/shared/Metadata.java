package shared;

import java.io.Serializable;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Metadata implements Serializable {

    private static final long serialVersionUID = 2654489690409479031L;

    static {
        try {
            // Check if MD5 algorithm exists
            MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new Error(Util.getStackTraceString(e));
        }
    }

    // sorted
    private List<ECSNode> servers;

    public Metadata(List<ECSNode> servers) {
        this.servers = new ArrayList<>(servers);
        Collections.sort(this.servers);
    }

    public List<ECSNode> getServers() {
        return servers;
    }

    public ECSNode getServer(String ringPosition) {
        if (servers.size() == 0) {
            return null;
        }
        return binarySearch(ringPosition);
    }

    public ECSNode getPredecessor(ECSNode node) {
        if (servers.size() == 0) {
            return null;
        }
        int index = -1;
        for (int i = 0; i < servers.size(); ++i) {
            if (servers.get(i) == node) {
                index = i;
                break;
            }
        }
        if (index == -1) {
            return null;
        }
        if (index == 0) {
            return servers.get(servers.size() - 1);
        }
        return servers.get(index - 1);
    }

    public ECSNode getSuccessor(ECSNode node){
        if (servers.size() == 0) {
            return null;
        }
        int index = -1;
        for (int i = 0; i < servers.size(); ++i) {
            if (servers.get(i) == node) {
                index = i;
                break;
            }
        }
        if (index == -1) {
            return null;
        }
        if (index == servers.size() - 1) {
            return servers.get(0);
        }
        return servers.get(index + 1);
    }

    public ECSNode binarySearch(String ringPosition) {
        int left = 0;
        int right = servers.size() - 1;
        while (left < right) {
            int mid = (left + right) / 2;
            if (ringPosition.compareTo(servers.get(mid).getPosition()) < 0) {
                // ringPosition < mid
                right = mid;
            } else if (ringPosition
                    .compareTo(servers.get(mid).getPosition()) == 0) {
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
                .compareTo(servers.get(left).getPosition()) > 0) {
            return servers.get(0);
        }

        return servers.get(left);
    }

    public static String getRingPosition(String key) {
        MessageDigest hashGenerator;
        try {
            hashGenerator = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new Error(Util.getStackTraceString(e));
        }
        byte[] bytes = hashGenerator.digest(key.getBytes());
        return String.format("%032x", new BigInteger(1, bytes));
    }

    @Override
    public String toString() {
        return "Metadata{" +
                "servers=" + servers +
                '}';
    }
}
