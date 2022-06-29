package org.apache.ignite.cli.core.flow;

public interface FlowElement<IT, OT> {
    FlowOutput<OT> call(IT input) throws FlowInterruptException;

    default <O> FlowElement<IT, O> composite(FlowElement<FlowOutput<OT>, O> next, FlowInterrupter<OT> flowInterrupter) {
        return input -> {
            FlowOutput<OT> output = FlowElement.this.call(input);
            try {
                return next.call(output);
            } catch (FlowInterruptException e) {
                flowInterrupter.interrupt(output);
            }
            return null;
        };
    }
}
