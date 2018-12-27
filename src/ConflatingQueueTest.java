import io.micrometer.core.instrument.Measurement;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConflatingQueueTest {
    private Logger logger = Logger.getAnonymousLogger();
    private MeterRegistry registry = new SimpleMeterRegistry();

    @BeforeEach
    public void setUp() {

    }

    @Test
    public void testPushAndPull() throws InterruptedException {
        ConflatingQueue uut = new ConflatingQueue();
        Object payload1 = new Object();
        Object payload2 = new Object();
        uut.push(new VanillaMessage(payload1));
        uut.push(new VanillaMessage(payload2));
        assertEquals(payload1, uut.take().getPayload());
        assertEquals(payload2, uut.take().getPayload());
    }

    @Test
    public void testPushAndPullConflatable() throws InterruptedException {
        ConflatingQueue uut = new ConflatingQueue();
        ConflatableMessage m = new ConflatableMessage("xyz123", new Object());
        uut.push(m);
        Object lastPayload = new Object();
        uut.push(new ConflatableMessage("xyz123", lastPayload));
        AbstractMessage m2 = uut.take();
        assertEquals("xyz123", ((ConflatableMessage) m2).getKey());
        assertEquals(lastPayload, m2.getPayload());
        assertTrue(uut.isEmpty());
    }

    @Test
    public void testPushAndPullConflatableWithSweep() throws InterruptedException {
        ConflatingQueue uut = new ConflatingQueue();
        ConflatableMessage m = new ConflatableMessage("xyz123", new Object());
        uut.push(m);
        Object lastPayload = new Object();
        uut.push(new ConflatableMessage("xyz123", lastPayload));
        AbstractMessage m2 = uut.take();
        uut.sweep();
        assertEquals("xyz123", ((ConflatableMessage) m2).getKey());
        assertEquals(lastPayload, m2.getPayload());
        assertTrue(uut.isEmpty());
    }

    @Test
    public void testPushAndPullConflatableMaintainsOrder() throws InterruptedException {
        ConflatingQueue uut = new ConflatingQueue();
        ConflatableMessage m = new ConflatableMessage("xyz123", new Object());
        uut.push(m);
        uut.push(new ConflatableMessage("abc123", new Object()));
        Object lastPayload = new Object();
        uut.push(new ConflatableMessage("xyz123", lastPayload));
        AbstractMessage m2 = uut.take();
        assertEquals("xyz123", ((ConflatableMessage) m2).getKey());
        assertEquals(lastPayload, m2.getPayload());
        assertEquals("abc123", ((ConflatableMessage) uut.take()).getKey());
        assertTrue(uut.isEmpty());
    }

    @Test
    public void testMultithreadedAccess() throws InterruptedException, ExecutionException {
        ConflatingQueue uut = new ConflatingQueue();
        AtomicInteger c = new AtomicInteger();
        ExecutorService tp = Executors.newFixedThreadPool(20);
        List<Future<Boolean>> results = new LinkedList<>();
        int iterations = 1000;
        Timer vanillaPushTimer = Timer
                .builder("push.vanilla.timer")
                .description("time to push vanilla messages") // optional
                .tags("push", "vanilla") // optional
                .register(registry);
        Timer conflatingPushTimer = Timer
                .builder("push.conflating.timer")
                .description("time to push conflating messages") // optional
                .tags("push", "conflating") // optional
                .register(registry);
        Timer conflateTimer = Timer
                .builder("conflate.timer")
                .description("time to conflate messages") // optional
                .tags("conflate", "conflating") // optional
                .register(registry);
        Timer pullTimer = Timer
                .builder("pull.timer")
                .description("time to pull messages") // optional
                .tags("pull", "all") // optional
                .register(registry);

        for (int i = 0; i < iterations; i++) {
            results.add(tp.submit((Callable) () -> {
                VanillaMessage m3 = null;
                Object payload2 = new Object();
                String key = null;
                try {
                    key = "abc" + Integer.toString(c.getAndIncrement());
                    Object payload1 = new Object();
                    VanillaMessage m = new VanillaMessage(payload1);

                    vanillaPushTimer.record(() -> uut.push(m));

                    return true;

                } catch (Exception e) {
                    logger.warning("Callable interrupted on " + key);
                }

                return false;
            }));
        }

        for (int i = 0; i < iterations; i++) {
            results.add(tp.submit((Callable) () -> {
                VanillaMessage m3 = null;
                Object payload2 = new Object();
                String key = null;
                try {
                    key = "abc" + Integer.toString(c.getAndIncrement());
                    Object payload1 = new Object();
                    ConflatableMessage m = new ConflatableMessage(key, payload1);

                    conflatingPushTimer.record(() -> uut.push(m));

                    ConflatableMessage m2 = new ConflatableMessage(key, payload2);

                    conflateTimer.record(() -> uut.push(m2));
                    return true;
                } catch (Exception e) {
                    logger.warning("Callable interrupted on " + key);
                }
                return false;
            }));
        }



        for (int i = 0; i < iterations * 2; i++) {
            results.add(tp.submit((Callable) () -> {
                        pullTimer.record(() -> {
                            uut.take();
                        });
                        return true;
                    }

            ));
        }
        tp.awaitTermination(5, TimeUnit.SECONDS);


        for (Future<Boolean> resultFuture : results) {

            assertTrue(resultFuture.get().booleanValue());

        }
        logger.info("Pull Report: mean " + pullTimer.mean(TimeUnit.MILLISECONDS) + "ms, max " + pullTimer.max(TimeUnit.MILLISECONDS));

        logger.info("Vanilla Push Report: mean " + vanillaPushTimer.mean(TimeUnit.MILLISECONDS) + "ms, max " + vanillaPushTimer.max(TimeUnit.MILLISECONDS));
        logger.info("Conflating Push Report: mean " + conflatingPushTimer.mean(TimeUnit.MILLISECONDS) + "ms, max " + conflatingPushTimer.max(TimeUnit.MILLISECONDS));
        logger.info("Conflate Report: mean " + conflateTimer.mean(TimeUnit.MILLISECONDS) + "ms, max " + conflateTimer.max(TimeUnit.MILLISECONDS));

        /*
        while(!uut.isEmpty()) {
            AbstractMessage mhl=uut.take();
            logger.info("On queue: "+mhl+" : "+mhl.getClass().getCanonicalName());
        }*/
        assertTrue(uut.isEmpty());
        tp.shutdownNow();
    }
}

class TestResult {
    public Long pushTime;
    public Long vanillaPushTime;
    public Long conflateTime;
    public Long pullTime;
    public boolean success;

    public TestResult(Long vanillaPushTime, Long pushTime, Long pullTime, Long conflateTime, boolean success) {
        this.pushTime = pushTime;
        this.conflateTime = conflateTime;
        this.pullTime = pullTime;
        this.success = success;
        this.vanillaPushTime = vanillaPushTime;
    }
}