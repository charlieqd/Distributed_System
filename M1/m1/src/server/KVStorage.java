package server;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class KVStorage implements IKVStorage {

    private static KVStorage current = null;

    private ConcurrentHashMap<String, ReentrantLock> fileLocks = new ConcurrentHashMap<>();

    private final KeyHashStrategy keyHashStrategy;

    public KVStorage(String rootPath, KeyHashStrategy keyHashStrategy) {
        if (current == null) {
            throw new IllegalStateException(
                    "Cannot have more than one KVStorage instance");
        }

        this.keyHashStrategy = keyHashStrategy;
        current = this;
    }

    @Override
    public String get(String key) {
        String hash = keyHashStrategy.hashKey(key);
        // Create lock if not present
        Lock lock = fileLocks.computeIfAbsent(hash, k -> new ReentrantLock());
        lock.lock();
        try {
            // TODO implement
            return "";
        } finally {
            lock.unlock();
            // Remove lock if no thread is holding the lock
            fileLocks.computeIfPresent(hash, (k, v) -> {
                return v.isLocked() ? null : v;
            });
        }
    }

    @Override
    public void put(String key, String value) {
        String hash = keyHashStrategy.hashKey(key);
        // Create lock if not present
        Lock lock = fileLocks.computeIfAbsent(hash, k -> new ReentrantLock());
        lock.lock();
        try {
            // TODO implement
        } finally {
            lock.unlock();
            // Remove lock if no thread is holding the lock
            fileLocks.computeIfPresent(hash, (k, v) -> {
                return v.isLocked() ? null : v;
            });
        }
    }
}
