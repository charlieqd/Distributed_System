package server;

import app_kvServer.IKVServer;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.concurrent.ConcurrentHashMap;

public class KVStorage implements IKVStorage {

    private static KVStorage current = null;

    private final ConcurrentHashMap<String, IKVFileStorage> files = new ConcurrentHashMap<>();

    private final KeyHashStrategy keyHashStrategy;
    private final String rootPath;

    private Cache<String, String> cache;

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
        if (current == null) {
            throw new IllegalStateException(
                    "Cannot have more than one KVStorage instance");
        }

        this.keyHashStrategy = keyHashStrategy;
        current = this;

        // set up cache
        if (cacheStrategy == IKVServer.CacheStrategy.FIFO) {
            cache = new FIFOCache<>(cacheSize);
        } else if (cacheStrategy == IKVServer.CacheStrategy.LRU) {
            cache = new LRUCache<>(cacheSize);
        } else {
            cache = null;
        }
    }

    @Override
    public String get(String key) throws IOException {
        IKVFileStorage fileStorage = getFileStorage(key);
        return fileStorage.read(key);
    }

    @Override
    public void put(String key, String value) throws IOException {
        IKVFileStorage fileStorage = getFileStorage(key);
        fileStorage.write(key, value);
    }

    private IKVFileStorage getFileStorage(String key) {
        String hash = keyHashStrategy.hashKey(key);
        return files.computeIfAbsent(hash, k -> new KVFileStorage(
                Paths.get(rootPath, hash).toString()));
    }
}
