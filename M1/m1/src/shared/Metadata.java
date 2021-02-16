package shared;

import java.util.ArrayList;

public class Metadata {

    // sorted
    private ArrayList<ECSNode> servers;

    public Metadata(ArrayList<ECSNode> servers) {
        this.servers = servers;
    }

    public ECSNode getServer(String key) {
        if (servers.size() == 0) {
            return null;
        }
        return binarySearch(key);
    }

    public ECSNode binarySearch(String key) {
        int left = 0;
        int right = servers.size() - 1;
        while (left < right) {
            int mid = (left + right) / 2;
            if (key.compareTo(servers.get(mid).getPosition()) < 0) {
                // key < mid
                right = mid;
            } else if (key.compareTo(servers.get(mid).getPosition()) == 0) {
                // key == mid
                return servers.get(mid);
            } else {
                // key > mid
                left = left + 1;
            }
        }

        if (left == 0) {
            return servers.get(0);
        }

        if (left == servers.size() - 1 && key
                .compareTo(servers.get(left).getPosition()) > 0) {
            return servers.get(0);
        }

        return servers.get(left);


    }

}
