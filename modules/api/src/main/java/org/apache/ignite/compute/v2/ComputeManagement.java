package org.apache.ignite.compute.v2;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

public interface ComputeManagement {

    CompletableFuture<Void> changeJobPriority(UUID uuid, Priority priority);

    default CompletableFuture<List<JobStatus>> list() {
        return list(s -> true);
    }

    CompletableFuture<List<JobStatus>> list(Predicate<JobStatus> filter);

    CompletableFuture<Void> cancelJob(UUID id);
}
