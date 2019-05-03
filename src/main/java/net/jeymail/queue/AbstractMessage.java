package net.jeymail.queue;

abstract public class AbstractMessage {
    protected Object payload;

    public AbstractMessage(Object payload) {
        this.payload = payload;
    }

    public void setPayload(Object payload) {
        this.payload = payload;
    }

    public Object getPayload() {
        return payload;
    }

    abstract public boolean isConflatable();

    abstract public String getKey();
}
