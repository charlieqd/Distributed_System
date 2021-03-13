package server;

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

    public KVStorageDelta(int logicalTime) {
        this.logicalTime = logicalTime;
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
        if (!values.containsKey(key)) {
            values.put(key, new Value());
        }
        values.get(key).set(value);
    }

    public Set<Map.Entry<String, Value>> getEntrySet() {
        return values.entrySet();
    }
}
