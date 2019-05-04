package net.jeymail.queue.conflating;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertTrue;

class ConflatingQueueTest {
    private final Logger logger = Logger.getAnonymousLogger();
    private final MeterRegistry registry = new SimpleMeterRegistry();

    @Test
    void testPushAndPull() {
        ConflatingQueue uut = new ConflatingQueue();
        Object payload1 = new Object();
        Object payload2 = new Object();
        uut.push(new VanillaMessage(payload1));
        uut.push(new VanillaMessage(payload2));
        Assertions.assertEquals(payload1, uut.take().getPayload());
        Assertions.assertEquals(payload2, uut.take().getPayload());
    }

    @Test
    void testPushAndPullConflatable() {
        ConflatingQueue uut = new ConflatingQueue();
        Object firstPayload = new Object();
        ConflatableMessage m = new ConflatableMessage("xyz123", firstPayload);
        uut.push(m);
        Object lastPayload = new Object();
        uut.push(new ConflatableMessage("xyz123", lastPayload));
        AbstractMessage m1 = uut.take();
        Assertions.assertEquals("xyz123", m1.getKey());
        Assertions.assertEquals(firstPayload, m1.getPayload());
        AbstractMessage m2 = uut.take();
        Assertions.assertEquals("xyz123", m2.getKey());
        Assertions.assertEquals(lastPayload, m2.getPayload());
        Assertions.assertTrue(uut.isEmpty());
    }

    @Test
    void testPushAndPullConflatableWithSweep() {
        ConflatingQueue uut = new ConflatingQueue();
        ConflatableMessage m = new ConflatableMessage("xyz123", new Object());
        uut.push(m);
        Object lastPayload = new Object();
        uut.push(new ConflatableMessage("xyz123", lastPayload));
        uut.sweep();
        AbstractMessage m2 = uut.take();
        Assertions.assertEquals("xyz123", m2.getKey());
        Assertions.assertEquals(lastPayload, m2.getPayload());
        Assertions.assertTrue(uut.isEmpty());
    }

    /* Order no longer preserved on conflation
        @Test
        void testPushAndPullConflatableMaintainsOrder() {
            ConflatingQueue uut = new ConflatingQueue();
            ConflatableMessage m = new ConflatableMessage("xyz123", new Object());
            uut.push(m);
            uut.push(new ConflatableMessage("abc123", new Object()));
            Object lastPayload = new Object();
            uut.push(new ConflatableMessage("xyz123", lastPayload));
            uut.sweep();
            AbstractMessage m2 = uut.take();
            Assertions.assertEquals("xyz123", m2.getKey());
            Assertions.assertEquals(lastPayload, m2.getPayload());
            Assertions.assertEquals("abc123", uut.take().getKey());
            Assertions.assertTrue(uut.isEmpty());
        }
    */
    @Test
    void testMultithreadedAccess() throws InterruptedException, ExecutionException {
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
                    key = "abc" + c.getAndIncrement();
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
                    key = "abc" + c.getAndIncrement();
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
        uut.sweep();
        for (int i = 0; i < iterations * 2; i++) {
            results.add(tp.submit((Callable) () -> {
                        pullTimer.record((Runnable) uut::drain);
                        return true;
                    }

            ));
        }
        tp.awaitTermination(5, TimeUnit.SECONDS);


        for (Future<Boolean> resultFuture : results) {

            assertTrue(resultFuture.get());

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
        Assertions.assertTrue(uut.isEmpty());
        tp.shutdownNow();
    }
}

class TestResult {

    public TestResult(Long vanillaPushTime, Long pushTime, Long pullTime, Long conflateTime, boolean success) {
    }
}