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

    private Object ecsCommandArg = null;

    public KVMessageImpl(String key, String value, StatusType status) {
        this(key, value, null, status);
    }

    public KVMessageImpl(String key,
                         String value,
                         Metadata metadata,
                         StatusType status) {
        this(key, value, metadata, status, null);
    }

    public KVMessageImpl(String key,
                         String value,
                         Metadata metadata,
                         StatusType status,
                         Object ecsCommandArg) {
        this.key = key;
        this.value = value;
        this.metadata = metadata;
        this.status = status;
        this.ecsCommandArg = ecsCommandArg;
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
    public Object getECSCommandArg() {
        return ecsCommandArg;
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
            case ECS_SUCCESS:
                return statusName;
            case ECS_START_SERVING:
                return statusName;
            case ECS_STOP_SERVING:
                return statusName;
            case ECS_SHUT_DOWN:
                return statusName;
            case ECS_LOCK_WRITE:
                return statusName;
            case ECS_UNLOCK_WRITE:
                return statusName;
            case ECS_MOVE_DATA:
                return statusName + "<(ecsCommandArg)>";
            case ECS_UPDATE_METADATA:
                return statusName + "<(metadata)>";
            case FAILED:
                return statusName + "<" + value + ">";
        }

        return "UNKNOWN<" + key + "," + value + ">";
    }
}
