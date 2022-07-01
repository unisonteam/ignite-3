package org.apache.ignite.cli.core.flow.builder;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import org.apache.ignite.cli.core.call.CallInput;
import org.apache.ignite.cli.core.flow.FlowElement;
import org.apache.ignite.cli.core.flow.FlowOutput;

public class FlowBranchExecutionPipelineBuilder<I extends CallInput, T> {
    private final FlowExecutionPipelineBuilder<I, T> flowBuilder;
    private final List<Branch<T>> branches = new ArrayList<>();

    public FlowBranchExecutionPipelineBuilder(FlowExecutionPipelineBuilder<I, T> flowBuilder) {
        this.flowBuilder = flowBuilder;
    }

    public FlowBranchExecutionPipelineBuilder<I, T> branch(Predicate<T> predicate, FlowElement<FlowOutput<T>, ?> flowElement) {
        branches.add(new Branch<>(predicate, flowElement));
        return this;
    }

    public FlowExecutionPipeline<I, ?> build() {
        return flowBuilder.appendFlow((input, interrupt) -> {
            T body = input.body();
            for (Branch<T> branch : branches) {
                if (branch.filter.test(body)) {
                    return branch.flowElement.call(input, interrupt);
                }
            }
            return null;
        }).build();
    }

    private static class Branch<I> {
        private final Predicate<I> filter;
        private final FlowElement<FlowOutput<I>, ?> flowElement;

        public Branch(Predicate<I> filter, FlowElement<FlowOutput<I>, ?> flowElement) {
            this.filter = filter;
            this.flowElement = flowElement;
        }
    }
}
