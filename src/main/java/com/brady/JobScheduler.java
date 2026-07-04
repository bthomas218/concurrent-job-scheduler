package com.brady;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;


public class JobScheduler {
    private final PriorityBlockingQueue<Job> queue;
    private final AtomicBoolean acceptingJobs = new AtomicBoolean(true);
    private final ExecutorService executor;
    private final ScheduledExecutorService scheduledExecutor;
    private static final int BASE_DELAY = 10;
    private static final System.Logger logger = System.getLogger(JobScheduler.class.getName());
    private final Set<ScheduledFuture<?>> scheduledFutures = ConcurrentHashMap.newKeySet();

    public JobScheduler(int initialCapacity, int workerCount) {
        if (initialCapacity <= 0) {
            logger.log(System.Logger.Level.TRACE, "initialCapacity <= 0");
            throw new IllegalArgumentException("Initial capacity must be greater than zero.");
        }

        if (workerCount <= 0) {
            logger.log(System.Logger.Level.TRACE, "workerCount <= 0");
            throw new IllegalArgumentException("Workers must be greater than zero.");
        }

        logger.log(System.Logger.Level.INFO, "Initializing JobScheduler with initial capacity: %d and %d workers".formatted(initialCapacity, workerCount));
        this.queue = new PriorityBlockingQueue<>(initialCapacity, Comparator.comparingInt(Job::priority).reversed().thenComparing(Job::id));

        this.executor = Executors.newVirtualThreadPerTaskExecutor();
        this.scheduledExecutor = Executors.newScheduledThreadPool(workerCount);

        logger.log(System.Logger.Level.INFO, "Starting workers");
        for (int i = 0; i < workerCount; i++) {
            executor.submit(this::workerLoop);
        }

    }

    public synchronized void submit(Job job) {
        if (job == null) {
            logger.log(System.Logger.Level.TRACE, "Job is null");
            throw new IllegalArgumentException("Job cannot be null.");
        }

        if (!acceptingJobs.get()) {
            logger.log(System.Logger.Level.TRACE, "Job submission while shutting down.");
            throw new IllegalStateException("Scheduler is shutting down.");
        }

        logger.log(System.Logger.Level.INFO, "Submitting Job %s (priority=%d)".formatted(job.id(), job.priority()));
        queue.add(job);

    }

    public synchronized void shutdown() {
        logger.log(System.Logger.Level.INFO, "Shutting down JobScheduler");
        acceptingJobs.set(false);
        queue.clear();

        for (ScheduledFuture<?> scheduledFuture : scheduledFutures) {
            scheduledFuture.cancel(false);
        }
        scheduledFutures.clear();

        executor.shutdown();
        scheduledExecutor.shutdown();
    }

    private void workerLoop() {
        while (acceptingJobs.get()) {
            try {
                Job job = queue.poll(100, TimeUnit.MILLISECONDS);
                if (job != null) {
                    logger.log(System.Logger.Level.INFO, "Executing job: %s".formatted(job.id()));
                    execute(job);
                }
            } catch (InterruptedException e) {
                logger.log(System.Logger.Level.ERROR, "Worker Interrupted: %s".formatted(e));
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void execute(Job job) {
        try {
            job.task().run();
        } catch (Exception e) {
            if (!job.hasNextAttempt()) {
                logger.log(System.Logger.Level.WARNING, "Job %s failed".formatted(job.id()), e);
                return;
            }

            long delay = calculateBackOff(job.attempt());
            logger.log(System.Logger.Level.INFO, "Scheduling retry %d/%d for job %s in %d ms".formatted(
                    job.attempt() + 1, job.maxRetries(), job.id(), delay));
            scheduleRetry(job, delay);
        }
    }

    private long calculateBackOff(int attempts) {
        return (long) (BASE_DELAY * Math.pow(2, attempts));
    }

    private void scheduleRetry(Job job, long delay) {
        AtomicReference<ScheduledFuture<?>> futureRef = new AtomicReference<>();

        Runnable retryTask = () -> {
            try {
                if (acceptingJobs.get()) {
                    queue.add(job.nextAttempt());
                }
            } finally {
                scheduledFutures.remove(futureRef.get());
            }
        };

        ScheduledFuture<?> future = scheduledExecutor.schedule(retryTask, delay, TimeUnit.MILLISECONDS);

        futureRef.set(future);
        scheduledFutures.add(future);
    }
}
