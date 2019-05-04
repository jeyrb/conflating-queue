package net.jeymail.queue.conflating;

abstract public class AbstractMessage {
    private Object payload;

    AbstractMessage(Object payload) {
        this.payload = payload;
    }

    public void setPayload(Object payload) {
        this.payload = payload;
    }

    public Object getPayload() {
        return payload;
    }

    abstract public String getKey();

    abstract public boolean isConflatable();
}
