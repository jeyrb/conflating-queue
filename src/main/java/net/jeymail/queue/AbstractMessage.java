package net.jeymail.queue;

abstract public class AbstractMessage {
    private Object payload;

    public AbstractMessage(Object payload) {
        this.payload = payload;
    }


    public Object getPayload() {
        return payload;
    }

    abstract public boolean isConflatable();

}
