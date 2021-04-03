package client;

public interface TransactionRunner {
    void run(KVStore kvStore) throws Exception;
}
