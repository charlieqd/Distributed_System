package server;

import app_kvServer.IKVServer;
import org.apache.log4j.Logger;
import shared.messages.KVMessage;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class KVStorage implements IKVStorage {

    private static final Logger logger = Logger.getRootLogger();

    private static final String NULL_VALUE = new String();

    private static KVStorage current = null;

    private final ConcurrentHashMap<String, IKVFileStorage> files = new ConcurrentHashMap<>();

    private final KeyHashStrategy keyHashStrategy;
    private final String rootPath;

    private final Cache<String, String> cache;

    private final Lock lock;

    /**
     * @param rootPath
     * @param keyHashStrategy
     * @param cacheSize       specifies how many key-value pairs the server is
     *                        allowed to keep in-memory
     * @param cacheStrategy   specifies the cache replacement strategy in case
     *                        the cache is full and there is a GET- or
     *                        PUT-request on a key that is currently not
     *                        contained in the cache.
     */
    public KVStorage(String rootPath, KeyHashStrategy keyHashStrategy,
                     int cacheSize, IKVServer.CacheStrategy cacheStrategy) {
        this.rootPath = rootPath;
        if (current != null) {
            throw new IllegalStateException(
                    "Cannot have more than one KVStorage instance");
        }

        // Make directories
        File file = new File(this.rootPath);
        if (!file.exists()) {
            file.mkdirs();
        }

        this.keyHashStrategy = keyHashStrategy;
        current = this;

        // set up cache
        if (cacheStrategy == IKVServer.CacheStrategy.FIFO) {
            cache = new FIFOCache<>(cacheSize);
        } else if (cacheStrategy == IKVServer.CacheStrategy.LRU) {
            cache = new LRUCache<>(cacheSize);
        } else if (cacheStrategy == IKVServer.CacheStrategy.None) {
            cache = new DummyCache<>();
        } else {
            throw new IllegalArgumentException(
                    "Unsupported cache strategy: " + cacheStrategy.toString());
        }

        lock = new ReentrantLock();
    }

    @Override
    public String get(String key) throws IOException {
        lock.lock();
        try {
            String value = cache.get(key);
            if (value == null) {
                IKVFileStorage fileStorage = getFileStorage(key);
                value = fileStorage.read(key);

                cache.put(key, value == null ? NULL_VALUE : value);
                logger.info("Cache missed for key \"" + key + "\"");
            }
            return value == NULL_VALUE ? null : value;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public KVMessage.StatusType put(String key, String value) throws
            IOException {
        lock.lock();
        try {
            IKVFileStorage fileStorage = getFileStorage(key);
            KVMessage.StatusType response = fileStorage.write(key, value);

            cache.put(key, value == null ? NULL_VALUE : value);

            return response;
        } finally {
            lock.unlock();
        }
    }

    private IKVFileStorage getFileStorage(String key) {
        String hash = keyHashStrategy.hashKey(key);
        return files.computeIfAbsent(hash, k -> new KVFileStorage(
                Paths.get(rootPath, hash).toString()));
    }
}
