package com.brady;

public record Job(
        String id,
        int priority,
        int attempt,
        int maxRetries,
        Runnable task
) {
    public Job {
        if (attempt < 0) {
            throw new IllegalArgumentException("Attempt cannot be negative");
        }
        if (maxRetries < 0) {
            throw new IllegalArgumentException("Max retries cannot be negative");
        }
        if(id == null || id.isBlank()) {
            throw new IllegalArgumentException("Job id cannot be null or blank");
        }
        if(task == null) {
            throw new IllegalArgumentException("Job task cannot be null");
        }
        if(attempt > maxRetries) {
            throw new IllegalArgumentException("Attempt cannot be greater than max retries");
        }
    }

    public boolean hasNextAttempt() {
        return attempt < maxRetries;
    }

    public Job nextAttempt() {
        return new Job(this.id, this.priority, this.attempt + 1, this.maxRetries, this.task);
    }
}
