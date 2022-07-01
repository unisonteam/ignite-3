package org.apache.ignite.cli.core.flow;

public class HandledFlowElementWith<IT, OT> implements FlowElement<IT, OT> {
    private final FlowElement<IT, OT> flowElement;
    private final InterruptHandler<IT> interruptHandler;

    public HandledFlowElementWith(FlowElement<IT, OT> flowElement, InterruptHandler<IT> interruptHandler) {
        this.flowElement = flowElement;
        this.interruptHandler = interruptHandler;
    }

    @Override
    public FlowOutput<OT> call(IT input, FlowInterrupter interrupt) {
        return flowElement.call(input, new FlowInterrupter.HandledFlowInterrupted<>(interruptHandler, interrupt));
    }
}
