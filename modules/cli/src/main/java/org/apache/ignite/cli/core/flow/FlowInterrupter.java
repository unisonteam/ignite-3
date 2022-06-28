package org.apache.ignite.cli.core.flow;

public interface FlowInterrupter<T> {
    void interrupt(T value);
}
