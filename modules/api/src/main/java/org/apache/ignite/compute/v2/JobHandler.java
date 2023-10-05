package org.apache.ignite.compute.v2;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.compute.v2.JobStatus;

public interface JobHandler<T> {
    T result();

    CompletableFuture<Void> changePriority(long priority);

    CompletableFuture<Void> cancel();

    CompletableFuture<JobStatus> status();
}
