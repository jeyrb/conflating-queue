package net.jeymail.queue.conflating;

public class VanillaMessage extends AbstractMessage {

    public VanillaMessage(Object payload) {
        super(payload);
    }

    @Override
    public String toString() {
        return "VanillaMessage{}";
    }

    @Override
    public String getKey() {
        return null;
    }

    @Override
    public boolean isConflatable() {
        return false;
    }
}
