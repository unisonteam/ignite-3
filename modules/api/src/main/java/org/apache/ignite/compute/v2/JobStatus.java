package org.apache.ignite.compute.v2;

import static org.apache.ignite.compute.v2.JobState.SUBMITTED;

import java.time.Instant;
import java.util.UUID;

public class JobStatus {
    private final UUID id;

    private JobState state;

    private String ownership;

    private Instant startTime;

    private Instant finishTime;


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
