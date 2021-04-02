package server;

import shared.Metadata;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Note: This class is not thread safe.
 */
public class KVStorageDelta {
    public static class Value {
        private String value = null;

        /**
         * The return value can be null to indicate a delete.
         */
        public String get() {
            return value;
        }

        /**
         * Value can be null to indicate a delete.
         */
        private void set(String value) {
            this.value = value;
        }
    }

    private Map<String, Value> values;

    private final int logicalTime;
    private final String hashRangeStart;
    private final String hashRangeEnd;

    public KVStorageDelta(int logicalTime,
                          String hashRangeStart,
                          String hashRangeEnd) {
        this.values = new HashMap<>();
        this.logicalTime = logicalTime;
        this.hashRangeStart = hashRangeStart;
        this.hashRangeEnd = hashRangeEnd;
    }

    public int getLogicalTime() {
        return logicalTime;
    }

    public int getEntryCount() {
        return values.size();
    }

    /**
     * Record a write operation; value can be null to indicate a delete.
     */
    public void put(String key, String value) {
        String keyHash = Metadata.getRingPosition(key);
        boolean isResponsible = false;
        int comp = hashRangeStart.compareTo(hashRangeEnd);
        if (comp > 0) {
            if (keyHash.compareTo(hashRangeStart) > 0 || keyHash
                    .compareTo(hashRangeEnd) <= 0) {
                isResponsible = true;
            }
        } else if (comp < 0) {
            if (keyHash.compareTo(hashRangeStart) > 0 && keyHash
                    .compareTo(hashRangeEnd) <= 0) {
                isResponsible = true;
            }
        } else { // Hash range start and end are equal
            isResponsible = true;
        }
        if (!isResponsible) {
            return;
        }

        if (!values.containsKey(key)) {
            values.put(key, new Value());
        }
        values.get(key).set(value);
    }

    public Set<Map.Entry<String, Value>> getEntrySet() {
        return values.entrySet();
    }

    /**
     * Record a get operation. return null if not exist
     */
    public String get(String key){
        // not finished, return ? if not exsit
        if(values.containsKey(key)){
            return values.get(key).get();
        }
        return null;
    }

    /**
     * Clear values
     */
    public void clear(){
        values.clear();
    }

    /**
     * Return all keys in values
     */
    public Set<String> getAllKeys(){
        return values.keySet();
    }
}
