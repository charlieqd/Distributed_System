package server;

public interface Cache<K, V> {
    /**
     * @return current size of thr cache
     */
    int getSize();

    /**
     * @return the maximum size of the cache
     */
    int getCapacity();

    V get(K key);

    void set(K key, V value);
}
