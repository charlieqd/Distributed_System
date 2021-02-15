package shared;

import java.util.ArrayList;

public class Metadata {

    // sorted
    private ArrayList<ServerInfo> servers;

    public Metadata(ArrayList<ServerInfo> servers) {
        this.servers = servers;
    }

    public ServerInfo getServer(String key) {
        if (servers.size() == 0) {
            return null;
        }
        return binarySearch(key);
    }

    public ServerInfo binarySearch(String key) {
        int left = 0;
        int right = servers.size() - 1;
        while (left < right) {
            int mid = (left + right) / 2;
            if (key.compareTo(servers.get(mid).position) < 0) {
                // key < mid
                right = mid;
            } else if (key.compareTo(servers.get(mid).position) == 0) {
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
                .compareTo(servers.get(left).position) > 0) {
            return servers.get(0);
        }

        return servers.get(left);


    }

}
