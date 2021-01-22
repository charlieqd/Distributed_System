package server;

public interface IKVFileStorage {

    String get(String key);

    void put(String key, String value);

}
