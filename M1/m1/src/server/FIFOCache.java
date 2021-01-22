package server;

import java.util.ArrayDeque;
import java.util.HashMap;

public class FIFOCache<K, V> implements Cache<K, V> {

    private ArrayDeque<K> queue = new ArrayDeque<>();
    private HashMap<K, V> dic = new HashMap<>();

    private int capacity = 0;

    public FIFOCache(int capacity) {
        this.capacity = capacity;
    }

    @Override
    public int getSize() {
        return dic.size();
    }

    @Override
    public int getCapacity() {
        return capacity;
    }

    @Override
    public V get(K key) {
        if (dic.containsKey(key)) {
            return dic.get(key);
        }
        return null;
    }

    @Override
    public void put(K key, V value) {
        if (dic.containsKey(key)) {
            dic.put(key, value);
            return;
        }
        dic.put(key, value);
        queue.addLast(key);

        if (getSize() > capacity) {
            K firstKey = queue.removeFirst();
            dic.remove(firstKey);
        }
    }
}
