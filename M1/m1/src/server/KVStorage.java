package server;

import app_kvServer.IKVServer;
import org.apache.log4j.Logger;
import shared.Util;
import shared.messages.KVMessage;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

public class KVStorage implements IKVStorage {

    private static final Logger logger = Logger.getRootLogger();

    private static final String NULL_VALUE = new String();

    private final ConcurrentHashMap<String, IKVFileStorage> files = new ConcurrentHashMap<>();

    private final KeyHashStrategy keyHashStrategy;
    private final String rootPath;

    private final Cache<String, String> cache;

    private final Lock lock;

    private KVStorageDelta delta = null;

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

        // Make directories
        File file = new File(this.rootPath);
        if (!file.exists()) {
            file.mkdirs();
        }

        this.keyHashStrategy = keyHashStrategy;

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
        logger.info("Cache strategy: " + cacheStrategy.toString());

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
                logger.debug("Cache missed for key \"" + key + "\"");
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

            if (delta != null) {
                delta.put(key, value);
            }

            return response;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void clearCache() {
        lock.lock();
        try {
            cache.clear();
        } finally {
            lock.unlock();
        }
    }

    private IKVFileStorage getFileStorage(String key) {
        String hash = keyHashStrategy.hashKey(key);
        return files.computeIfAbsent(hash, k -> new KVFileStorage(
                Paths.get(rootPath, hash).toString()));
    }

    public List<String> getAllKeys(String hashRangeStart,
                                   String hashRangeEnd) throws IOException {
        lock.lock();
        try (Stream<Path> paths = Files.walk(Paths.get(this.rootPath))) {

            Object[] files = paths.filter(Files::isRegularFile)
                    .filter(path -> {
                        String fName = path.getFileName().toString();
                        int prefixLength = fName.length();
                        String startPrefix = hashRangeStart
                                .substring(0, prefixLength);
                        String endPrefix = hashRangeEnd
                                .substring(0, prefixLength);

                        if (hashRangeStart.compareTo(hashRangeEnd) > 0) {
                            if (fName.compareTo(startPrefix) >= 0 || fName
                                    .compareTo(endPrefix) <= 0) {
                                return true;
                            }
                        } else {
                            if (fName.compareTo(startPrefix) >= 0 && fName
                                    .compareTo(endPrefix) <= 0) {
                                return true;
                            }
                        }
                        return false;
                    }).toArray();

            ArrayList<String> keys = new ArrayList<>();

            for (Object f : files) {
                KVFileStorage storage = new KVFileStorage(
                        ((Path) f).toString());
                Util.concatenateArrayLists(keys,
                        storage.readKeys(hashRangeStart, hashRangeEnd));

            }
            return keys;

        } finally {
            lock.unlock();
        }
    }

    @Override
    public Integer getCurrentDeltaLogicalTime() {
        lock.lock();
        try {
            if (delta == null) {
                return null;
            } else {
                return delta.getLogicalTime();
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public KVStorageDelta startNextDeltaRecording(int logicalTime) {
        lock.lock();
        try {
            KVStorageDelta lastDelta = delta;
            delta = new KVStorageDelta(logicalTime);
            return lastDelta;
        } finally {
            lock.unlock();
        }
    }

}
