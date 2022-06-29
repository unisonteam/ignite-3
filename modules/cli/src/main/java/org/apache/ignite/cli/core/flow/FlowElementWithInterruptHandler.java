package org.apache.ignite.cli.core.flow;

public class FlowElementWithInterruptHandler<IT, OT> implements FlowElement<IT, OT> {
    private final InterruptHandler<IT> interruptHandler;

    public FlowElementWithInterruptHandler(InterruptHandler<IT> interruptHandler) {
        this.interruptHandler = interruptHandler;
    }

    @Override
    public FlowOutput<OT> call(IT input, FlowInterrupter interrupt) {
        return null;
    }
}
