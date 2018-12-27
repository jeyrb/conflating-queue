package net.jeymail.conflatingqueue;

public class VanillaMessage extends AbstractMessage {

    public VanillaMessage(Object payload) {
        super(payload);
    }

    public boolean isConflatable() {
        return false;
    }

    @Override
    public String toString() {
        return "net.jeymail.conflatingqueue.VanillaMessage{}";
    }
}
