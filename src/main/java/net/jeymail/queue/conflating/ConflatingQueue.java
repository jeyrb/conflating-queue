package net.jeymail.queue.conflating;

import net.jeymail.queue.AbstractMessage;
import net.jeymail.queue.ConflatableMessage;
import net.jeymail.queue.VanillaMessage;

import java.util.Collection;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

/**
 * Conflating queue, suitable for low cardinality conflation keys
 */
class ConflatingQueue {

    private Logger logger = Logger.getAnonymousLogger();

    private final LinkedBlockingQueue<AbstractMessage> queue;
    private final ConcurrentHashMap<String, AbstractMessage> headIndex;
    private final Object mutex;


    public ConflatingQueue() {
        this.mutex = new Object();
        this.queue = new LinkedBlockingQueue<>();
        this.headIndex = new ConcurrentHashMap<>();
    }

    /**
     * Goal - minimum time to push, since on non-consumer shared thread
     * Conflate opportunistically to minimize memory and secondarily network traffic to consumers
     *
     * @param m
     */
    public void push(VanillaMessage m) {
        this.queue.add(m);
    }

    /**
     * Goal - minimum time to push, since on non-consumer shared thread
     * Conflate opportunistically to minimize memory and secondarily network traffic to consumers
     *
     * @param m
     */
    public void push(ConflatableMessage m) {
        synchronized (this.mutex) {
            this.queue.add(m);
            this.headIndex.put(m.getKey(), m);
        }
    }

    /**
     * Run from periodic timer, aimed at slow consumers only to control memory usage
     */
    public void sweep() {
        synchronized (this.mutex) {
            if (this.queue.removeIf(m -> m.isConflatable()
                    && this.headIndex.getOrDefault(m.getKey(), m) != m)
            ) {
                logger.info("Swept conflated messages");
            }
        }
    }

    public AbstractMessage take()  {
        try {
            return this.queue.take();
        } catch (InterruptedException e) {
            throw new RuntimeException("interrupted");
        }

    }

    public Collection<AbstractMessage> drain() {
        Collection<AbstractMessage> buffer = new LinkedList<>();
        this.queue.drainTo(buffer);
        return buffer;
    }

    public boolean isEmpty() {
        return this.queue.isEmpty();
    }
}
