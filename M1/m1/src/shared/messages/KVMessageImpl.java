package shared.messages;

import java.io.Serializable;

public class KVMessageImpl implements KVMessage, Serializable {

    private static final long serialVersionUID = 966087689327304835L;

    private String key;
    private String value;
    private StatusType status;

    public KVMessageImpl(String key, String value, StatusType status) {
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
            case FAILED:
                return statusName + "<" + value + ">";
        }

        return "UNKNOWN<" + key + "," + value + ">";
    }
}
