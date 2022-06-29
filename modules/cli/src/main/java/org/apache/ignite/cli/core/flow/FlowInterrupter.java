package org.apache.ignite.cli.core.flow;

public interface FlowInterrupter {

    void interrupt(FlowOutput<?> flowOutput);
}
