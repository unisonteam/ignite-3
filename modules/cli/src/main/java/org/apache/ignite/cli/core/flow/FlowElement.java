package org.apache.ignite.cli.core.flow;

public interface FlowElement<IT, OT> {
    FlowOutput<OT> call(IT input, FlowInterrupter interrupt);

    default <O> FlowElement<IT, O> composite(FlowElement<FlowOutput<OT>, O> next) {
        return (input, interrupt) -> {
            FlowOutput<OT> output = FlowElement.this.call(input, interrupt);
            return next.call(output, interrupt);
        };
    }
}
