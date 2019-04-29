package net.jeymail.queue;

import net.jeymail.queue.AbstractMessage;

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
        return "ConflatableMessage{" +
                "key='" + key + '\'' +
                '}';
    }
}
