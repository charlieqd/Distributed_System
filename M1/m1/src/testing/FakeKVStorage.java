package testing;

import server.IKVStorage;
import shared.messages.KVMessage;

import java.util.concurrent.ConcurrentHashMap;

public class FakeKVStorage implements IKVStorage {
    private ConcurrentHashMap<String, String> map = new ConcurrentHashMap<>();

    @Override
    public String get(String key) {
        return map.get(key);
    }

    @Override
    public KVMessage.StatusType put(String key, String value) {
        if (value == null) {
            String prev = map.remove(key);
            if (prev == null) {
                return KVMessage.StatusType.DELETE_ERROR;
            } else {
                return KVMessage.StatusType.DELETE_SUCCESS;
            }
        } else {
            String prev = map.put(key, value);
            if (prev == null) {
                return KVMessage.StatusType.PUT_SUCCESS;
            } else {
                return KVMessage.StatusType.PUT_UPDATE;
            }
        }
    }

    @Override
    public void clearCache() {
    }
}
