package net.jeymail.queue.conflating;

import net.jeymail.queue.AbstractMessage;
import net.jeymail.queue.ConflatableMessage;
import net.jeymail.queue.VanillaMessage;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

/**
 * Conflating queue, suitable for low cardinality conflation keys
 */
public class ConflatingQueue {

    private Logger logger = Logger.getAnonymousLogger();

    private LinkedBlockingQueue<AbstractMessage> queue;
    private ConcurrentHashMap<String, AbstractMessage> index;
    private Object mutex;


    public ConflatingQueue() {
        this.mutex = new Object();
        this.queue = new LinkedBlockingQueue<>();
        this.index = new ConcurrentHashMap<>();
    }

    public void push(ConflatableMessage m) {
        synchronized (mutex) {
            AbstractMessage xm = this.index.get(m.getKey());
            if (xm != null) {
                    xm.setPayload(m.getPayload());
                    return;
            } else {
                this.queue.add(m);
                this.index.put(m.getKey(), m);
            }
        }
    }


    public void push(VanillaMessage m) {
        this.queue.add(m);
    }

    public AbstractMessage take()  {
            try {
                return this.queue.take();
            } catch (InterruptedException e) {
                throw new RuntimeException("interrupted");
            }

    }

    public boolean isEmpty() {
        return this.queue.isEmpty();
    }
}
