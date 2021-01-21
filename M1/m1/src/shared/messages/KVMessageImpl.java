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
}
