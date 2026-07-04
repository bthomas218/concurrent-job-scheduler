package com.brady;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JobSchedulerTest {

    @Test
    void jobsExecuteInPriorityOrder() throws Exception {
        JobScheduler scheduler = new JobScheduler(10, 1);
        CountDownLatch blockerStarted = new CountDownLatch(1);
        CountDownLatch releaseBlocker = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(3);
        List<String> executed = new CopyOnWriteArrayList<>();

        try {
            scheduler.submit(new Job("blocker", 0, 0, 0, () -> {
                blockerStarted.countDown();
                await(releaseBlocker);
            }));

            assertTrue(blockerStarted.await(1, TimeUnit.SECONDS));

            scheduler.submit(new Job("low", 1, 0, 0, () -> {
                executed.add("low");
                finished.countDown();
            }));
            scheduler.submit(new Job("high", 10, 0, 0, () -> {
                executed.add("high");
                finished.countDown();
            }));
            scheduler.submit(new Job("medium", 5, 0, 0, () -> {
                executed.add("medium");
                finished.countDown();
            }));

            releaseBlocker.countDown();

            assertTrue(finished.await(1, TimeUnit.SECONDS));
            assertEquals(List.of("high", "medium", "low"), executed);
        } finally {
            scheduler.shutdown();
        }
    }

    @Test
    void failedJobsRetryTheCorrectNumberOfTimes() throws Exception {
        JobScheduler scheduler = new JobScheduler(10, 1);
        AtomicInteger attempts = new AtomicInteger();
        CountDownLatch allAttempts = new CountDownLatch(4);

        try {
            scheduler.submit(new Job("retrying", 1, 0, 3, () -> {
                attempts.incrementAndGet();
                allAttempts.countDown();
                throw new RuntimeException("try again");
            }));

            assertTrue(allAttempts.await(1, TimeUnit.SECONDS));
            assertEquals(4, attempts.get());
        } finally {
            scheduler.shutdown();
        }
    }

    @Test
    void retriesBackOffAsExpected() throws Exception {
        JobScheduler scheduler = new JobScheduler(10, 1);
        List<Long> startedAtNanos = new CopyOnWriteArrayList<>();
        CountDownLatch allAttempts = new CountDownLatch(3);

        try {
            scheduler.submit(new Job("backoff", 1, 0, 2, () -> {
                startedAtNanos.add(System.nanoTime());
                allAttempts.countDown();
                throw new RuntimeException("try again");
            }));

            assertTrue(allAttempts.await(1, TimeUnit.SECONDS));

            long firstDelayMillis = elapsedMillis(startedAtNanos.get(0), startedAtNanos.get(1));
            long secondDelayMillis = elapsedMillis(startedAtNanos.get(1), startedAtNanos.get(2));

            assertTrue(firstDelayMillis >= 8, "first retry should wait about 10ms");
            assertTrue(secondDelayMillis >= 18, "second retry should wait about 20ms");
            assertTrue(secondDelayMillis > firstDelayMillis, "backoff should increase between retries");
        } finally {
            scheduler.shutdown();
        }
    }

    @Test
    void shutdownRejectsNewSubmissions() {
        JobScheduler scheduler = new JobScheduler(10, 1);

        scheduler.shutdown();

        assertThrows(IllegalStateException.class,
                () -> scheduler.submit(new Job("late", 1, 0, 0, () -> {
                })));
    }

    @Test
    void pendingRetriesAreCanceledOnShutdown() throws Exception {
        JobScheduler scheduler = new JobScheduler(10, 1);
        AtomicInteger attempts = new AtomicInteger();
        CountDownLatch firstAttempt = new CountDownLatch(1);

        scheduler.submit(new Job("pending-retry", 1, 5, 6, () -> {
            attempts.incrementAndGet();
            firstAttempt.countDown();
            throw new RuntimeException("try again");
        }));

        assertTrue(firstAttempt.await(1, TimeUnit.SECONDS));

        scheduler.shutdown();
        Thread.sleep(450);

        assertEquals(1, attempts.get());
    }

    @Test
    void workersFinishInFlightJobBeforeExiting() throws Exception {
        JobScheduler scheduler = new JobScheduler(10, 1);
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(1);

        scheduler.submit(new Job("in-flight", 1, 0, 0, () -> {
            started.countDown();
            await(release);
            finished.countDown();
        }));

        assertTrue(started.await(1, TimeUnit.SECONDS));

        scheduler.shutdown();
        release.countDown();

        assertTrue(finished.await(1, TimeUnit.SECONDS));
    }

    private static long elapsedMillis(long startedAtNanos, long finishedAtNanos) {
        return TimeUnit.NANOSECONDS.toMillis(finishedAtNanos - startedAtNanos);
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
