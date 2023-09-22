package org.apache.ignite.compute.v2;

import static org.apache.ignite.compute.v2.JobState.SUBMITTED;

import java.util.UUID;

public class JobStatus {
    private final UUID id;

    private JobState state;

    public JobStatus(UUID id) {
        this.id = id;
        state = SUBMITTED;
    }

    public UUID id() {
        return id;
    }

    public JobState state() {
        return state;
    }
}
