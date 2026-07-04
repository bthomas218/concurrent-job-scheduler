package com.brady;

import java.util.Comparator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class JobScheduler {
    private final PriorityBlockingQueue<Job> queue;
    private final AtomicBoolean acceptingJobs = new AtomicBoolean(true);
    private final ExecutorService executor;

    public JobScheduler(int initialCapacity, int workerCount) {
        if (initialCapacity <= 0) {
            throw new IllegalArgumentException("Initial capacity must be greater than zero.");
        }

        if(workerCount <= 0) {
            throw new IllegalArgumentException("Workers must be greater than zero.");
        }

        this.queue = new PriorityBlockingQueue<>(initialCapacity,
                Comparator.comparingInt(Job::priority)
                        .reversed()
                        .thenComparing(Job::id));

        this.executor = Executors.newVirtualThreadPerTaskExecutor();

        for (int i = 0; i < workerCount; i++) {
            executor.submit(this::workerLoop);
        }

    }

    public synchronized void submit(Job job) {
        if (job == null) {
            throw new IllegalArgumentException("Job cannot be null.");
        }

        if (!acceptingJobs.get()) {
            throw new IllegalStateException("Scheduler is shutting down.");
        }

        queue.add(job);

    }

    public synchronized void shutdown() {
        acceptingJobs.set(false);
        queue.clear();
        executor.shutdown();
    }

    private void workerLoop() {
        while (acceptingJobs.get()) {
            try {
                Job job = queue.poll(100, TimeUnit.MILLISECONDS);
                if (job != null) {
                    execute(job);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void execute(Job job) {
        try {
            job.task().run();
        } catch(Exception e) {
            System.err.println("Error running job: " + job.id());
        }
    }
}
