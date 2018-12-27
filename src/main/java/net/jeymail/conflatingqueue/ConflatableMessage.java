package net.jeymail.conflatingqueue;

public class ConflatableMessage extends AbstractMessage {
    private String key;
    public ConflatableMessage(String key, Object payload) {
        super(payload);
        this.key = key;
    }

    @Override
    public boolean isConflatable() {
        return true;
    }

    public String getKey() {
        return key;
    }

    @Override
    public String toString() {
        return "net.jeymail.conflatingqueue.ConflatableMessage{" +
                "key='" + key + '\'' +
                '}';
    }
}
