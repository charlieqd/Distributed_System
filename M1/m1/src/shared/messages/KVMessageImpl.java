package shared.messages;

import shared.Metadata;

import java.io.Serializable;

public class KVMessageImpl implements KVMessage, Serializable {

    private static final long serialVersionUID = 966087689327304835L;

    public static final int MAX_KEY_LENGTH = 20;
    public static final int MAX_VALUE_LENGTH = 120_000;

    private String key;
    private String value;
    private Metadata metadata;
    private StatusType status;

    public KVMessageImpl(String key, String value, StatusType status) {
        this.key = key;
        this.value = value;
        this.metadata = null;
        this.status = status;
    }

    public KVMessageImpl(String key,
                         String value,
                         Metadata metadata,
                         StatusType status) {
        this.key = key;
        this.value = value;
        this.status = status;
    }

    @Override
    public String getKey() {
        return key;
    }

    @Override
    public String getValue() {
        return value;
    }

    public Metadata getMetadata() {
        return metadata;
    }

    @Override
    public StatusType getStatus() {
        return status;
    }

    @Override
    public String toString() {
        KVMessage.StatusType status = getStatus();
        String key = getKey();
        String value = getValue();
        if (status == null) return "UNKNOWN<" + key + "," + value + ">";
        String statusName = status.name();

        switch (status) {
            case CONNECTED:
                return statusName + "<(metadata)>";
            case DISCONNECT:
                return statusName;
            case GET:
                return statusName + "<" + key + ">";
            case GET_ERROR:
                return statusName + "<" + key + ">";
            case GET_SUCCESS:
                return statusName + "<" + key + "," + value + ">";
            case PUT:
                return statusName + "<" + key + "," + value + ">";
            case PUT_SUCCESS:
                return statusName + "<" + key + "," + value + ">";
            case PUT_UPDATE:
                return statusName + "<" + key + "," + value + ">";
            case PUT_ERROR:
                return statusName + "<" + key + "," + value + ">";
            case DELETE_SUCCESS:
                return statusName + "<" + key + ">";
            case DELETE_ERROR:
                return statusName + "<" + key + ">";
            case NOT_RESPONSIBLE:
                return statusName + "<(metadata)>";
            case SERVER_WRITE_LOCK:
                return statusName;
            case SERVER_STOPPED:
                return statusName;
            case FAILED:
                return statusName + "<" + value + ">";
        }

        return "UNKNOWN<" + key + "," + value + ">";
    }
}
