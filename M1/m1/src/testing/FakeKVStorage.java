package testing;

import server.IKVStorage;

import java.util.concurrent.ConcurrentHashMap;

public class FakeKVStorage implements IKVStorage {
    private ConcurrentHashMap<String, String> map = new ConcurrentHashMap<>();

    @Override
    public String get(String key) {
        return map.get(key);
    }

    @Override
    public void put(String key, String value) {
        if (value == null) {
            map.remove(key);
        } else {
            map.put(key, value);
        }
    }
}
