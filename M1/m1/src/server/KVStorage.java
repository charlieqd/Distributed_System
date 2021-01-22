package server;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.concurrent.ConcurrentHashMap;

public class KVStorage implements IKVStorage {

    private static KVStorage current = null;

    private final ConcurrentHashMap<String, IKVFileStorage> files = new ConcurrentHashMap<>();

    private final KeyHashStrategy keyHashStrategy;
    private final String rootPath;

    public KVStorage(String rootPath, KeyHashStrategy keyHashStrategy) {
        this.rootPath = rootPath;
        if (current == null) {
            throw new IllegalStateException(
                    "Cannot have more than one KVStorage instance");
        }

        this.keyHashStrategy = keyHashStrategy;
        current = this;
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
