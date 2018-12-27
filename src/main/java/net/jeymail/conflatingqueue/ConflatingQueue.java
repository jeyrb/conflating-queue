package net.jeymail.conflatingqueue;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

/**
 * Conflating queue, suitable for low cardinality conflation keys
 */
public class ConflatingQueue {

    private Logger logger = Logger.getAnonymousLogger();

    private LinkedBlockingQueue<MessageHolder> queue;
    private ConcurrentHashMap<String, MessageHolder> index;
    private Object mutex;


    public ConflatingQueue() {
        this.mutex = new Object();
        this.queue = new LinkedBlockingQueue<>();
        this.index = new ConcurrentHashMap<String, MessageHolder>();
    }

    public void push(ConflatableMessage m) {
        synchronized (mutex) {
            MessageHolder mh = this.index.get(m.getKey());
            if (mh != null) {
                if (this.queue.contains(mh)) { // could be slow
                    mh.setMessage(m);
                    return;
                }
            }
            mh = new MessageHolder(m);
            this.queue.add(mh);
            this.index.put(m.getKey(), mh);
        }
    }

    public void sweep() {
        synchronized (mutex) {
            if( this.queue.isEmpty()) {
                this.index.clear();
                logger.info("SWEEP Cleared index");
            } else {
                this.queue.stream()
                        .filter(MessageHolder::isConflatable)
                        .forEach(v->this.index.remove(((ConflatableMessage)v.getMessage()).getKey()));
            }
        }
    }

    public void push(VanillaMessage m) {
        this.queue.add(new MessageHolder(m));
    }

    public AbstractMessage take()  {
        try {
            return this.queue.take().getMessage();
        } catch (InterruptedException e) {
            throw new RuntimeException("interrupted");
        }
    }

    public boolean isEmpty() {
        return this.queue.isEmpty();
    }
}

class MessageHolder {
    private AbstractMessage message;

    public MessageHolder(AbstractMessage message) {
        this.message = message;
    }

    public AbstractMessage getMessage() {
        return message;
    }

    public void setMessage(AbstractMessage message) {
        this.message = message;
    }
    public boolean isConflatable() {
        return this.message.isConflatable();
    }
}