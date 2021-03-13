package server;

import shared.messages.KVMessage;

import java.io.IOException;
import java.util.List;

/**
 * NOTE: Methods must be thread-safe.
 */
public interface IKVStorage {
    String get(String key) throws IOException;

    KVMessage.StatusType put(String key, String value) throws IOException;

    void clearCache();

    List<String> getAllKeys(String hashRangeStart,
                            String hashRangeEnd) throws IOException;

    /**
     * @return The logical time of when the current delta started recording;
     * null if no delta is currently being recorded.
     */
    Integer getCurrentDeltaLogicalTime();

    /**
     * Returns the currently recorded delta, and start the next recording given
     * the current logical time. The next delta will have the given logical time
     * (returned when the next getCurrentDeltaLogicalTime() is called).
     */
    KVStorageDelta startNextDeltaRecording(int logicalTime);
}
