package net.jeymail.queue.conflating;

public class ConflatableMessage extends AbstractMessage {
    private final String key;

    /**
     * @param key     Must be globally unique across all instances of conflatablemessage
     * @param payload
     */
    public ConflatableMessage(String key, Object payload) {
        super(payload);
        this.key = key;
    }


    public String getKey() {
        return key;
    }

    @Override
    public boolean isConflatable() {
        return true;
    }

    @Override
    public String toString() {
        return "ConflatableMessage{" +
                "key='" + key + '\'' +
                '}';
    }
}
