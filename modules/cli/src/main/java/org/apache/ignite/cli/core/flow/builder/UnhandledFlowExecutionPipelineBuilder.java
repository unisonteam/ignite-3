package org.apache.ignite.cli.core.flow.builder;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.function.Supplier;
import org.apache.ignite.cli.core.call.CallInput;
import org.apache.ignite.cli.core.exception.ExceptionHandler;
import org.apache.ignite.cli.core.exception.ExceptionHandlers;
import org.apache.ignite.cli.core.flow.FlowElement;
import org.apache.ignite.cli.core.flow.HandledFlowElementWith;
import org.apache.ignite.cli.core.flow.InterruptHandler;

public class UnhandledFlowExecutionPipelineBuilder<I extends CallInput, O> extends FlowExecutionPipelineBuilder<I, O> {

    public UnhandledFlowExecutionPipelineBuilder(FlowElement<I, O> flow) {
        super(flow);
    }

    public FlowExecutionPipelineBuilder<I, O> interruptHandler(InterruptHandler<I> handler) {
        return new FlowExecutionPipelineBuilder<>(new HandledFlowElementWith<>(flow, handler))
                .inputProvider(inputProvider)
                .exceptionHandlers(exceptionHandlers)
                .errOutput(errOutput)
                .output(output);
    }


    @Override
    public UnhandledFlowExecutionPipelineBuilder<I, O> inputProvider(Supplier<I> inputProvider) {
        super.inputProvider(inputProvider);
        return this;
    }

    @Override
    public UnhandledFlowExecutionPipelineBuilder<I, O> output(PrintWriter output) {
        super.output(output);
        return this;
    }

    @Override
    public UnhandledFlowExecutionPipelineBuilder<I, O> output(OutputStream output) {
        super.output(output);
        return this;
    }

    @Override
    public UnhandledFlowExecutionPipelineBuilder<I, O> errOutput(PrintWriter errOutput) {
        super.errOutput(errOutput);
        return this;
    }

    @Override
    public UnhandledFlowExecutionPipelineBuilder<I, O> errOutput(OutputStream output) {
        super.errOutput(output);
        return this;
    }

    @Override
    public UnhandledFlowExecutionPipelineBuilder<I, O> exceptionHandler(ExceptionHandler<?> exceptionHandler) {
        super.exceptionHandler(exceptionHandler);
        return this;
    }

    @Override
    public UnhandledFlowExecutionPipelineBuilder<I, O> exceptionHandlers(ExceptionHandlers exceptionHandlers) {
        super.exceptionHandlers(exceptionHandlers);
        return this;
    }
}
