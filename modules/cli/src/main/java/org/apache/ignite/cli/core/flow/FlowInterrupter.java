package org.apache.ignite.cli.core.flow;

import org.apache.ignite.cli.commands.decorators.core.Decorator;
import org.apache.ignite.cli.commands.decorators.core.TerminalOutput;

public interface FlowInterrupter<T> {

    static FlowInterrupter<String> identity() {
        return value -> value;
    }

    static <T> FlowInterrupter<T> build(Decorator<T, TerminalOutput> decorator) {
        return value -> value.transform(decorator);
    }

    FlowOutput<String> interrupt(FlowOutput<T> value);
}
