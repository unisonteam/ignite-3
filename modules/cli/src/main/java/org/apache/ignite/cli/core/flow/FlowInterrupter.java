package org.apache.ignite.cli.core.flow;

public interface FlowInterrupter {
    void interrupt(FlowOutput<?> flowOutput);



    class HandledFlowInterrupted<T> implements FlowInterrupter {
        private final InterruptHandler<T> handler;
        private final FlowInterrupter rootInterrupter;

        public HandledFlowInterrupted(InterruptHandler<T> handler, FlowInterrupter interrupter) {
            this.handler = handler;
            rootInterrupter = findRootInterrupter(interrupter);
        }

        private static FlowInterrupter findRootInterrupter(FlowInterrupter interrupter) {
            if (interrupter instanceof HandledFlowInterrupted) {
                return ((HandledFlowInterrupted<?>) interrupter).rootInterrupter;
            }
            return interrupter;
        }

        @Override
        public void interrupt(FlowOutput<?> flowOutput) {
            handler.handle((FlowOutput<T>) flowOutput);

            rootInterrupter.interrupt(flowOutput);
        }
    }
}
