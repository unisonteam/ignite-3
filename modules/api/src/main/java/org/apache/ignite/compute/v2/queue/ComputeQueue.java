package org.apache.ignite.compute.v2.queue;

import java.util.Queue;

public interface ComputeQueue<T> extends Queue<T> {

    void addWithPriority(T e, long priority);
}
