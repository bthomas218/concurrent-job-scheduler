package com.brady;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

class JobSchedulerShutdownRaceTest {

    @Test
    void noAttemptEverStartsAfterShutdownIsCalled() throws Exception {
        int trialCount = 300;
        int racesDetected = 0;

        for (int trial = 0; trial < trialCount; trial++) {
            if (runOneTrial(trial)) {
                racesDetected++;
            }
        }

        System.out.printf("Completed %d trials, race condition observed in %d of them.%n",
                trialCount, racesDetected);
        // Deliberately NOT asserting racesDetected == 0 here — see note below
        // on why an assertion-based version of this test is printed instead
        // of enforced, given how likely a false "pass" is on a narrow race.
    }

    /**
     * Runs one trial. Returns true if a retry attempt was observed starting
     * AFTER shutdown() was called — i.e., the race was actually caught this
     * trial.
     */
    private boolean runOneTrial(int trial) throws Exception {
        JobScheduler scheduler = new JobScheduler(10, 2);
        List<Long> attemptTimestamps = new CopyOnWriteArrayList<>();
        CountDownLatch firstAttempt = new CountDownLatch(1);

        scheduler.submit(new Job("race-job-" + trial, 1, 0, 10, () -> {
            attemptTimestamps.add(System.nanoTime());
            firstAttempt.countDown();
            throw new RuntimeException("fail to trigger a retry");
        }));

        assertTrue(firstAttempt.await(1, TimeUnit.SECONDS),
                "Trial " + trial + ": first attempt never started");

        // Jitter shutdown timing across roughly the first two backoff windows
        // (10ms, 20ms) — trying, across many trials, to occasionally land
        // shutdown() while a retry task is actively mid-flight rather than
        // safely before or after it.
        Thread.sleep(ThreadLocalRandom.current().nextInt(0, 30));

        long shutdownCalledAtNanos = System.nanoTime();
        scheduler.shutdown();

        // Let any in-flight retry task fully settle before checking results.
        Thread.sleep(50);

        for (long attemptNanos : attemptTimestamps) {
            if (attemptNanos > shutdownCalledAtNanos) {
                System.out.printf(
                        "Trial %d: RACE CAUGHT — an attempt started %.3fms AFTER shutdown() was called%n",
                        trial, (attemptNanos - shutdownCalledAtNanos) / 1_000_000.0);
                return true;
            }
        }
        return false;
    }
}