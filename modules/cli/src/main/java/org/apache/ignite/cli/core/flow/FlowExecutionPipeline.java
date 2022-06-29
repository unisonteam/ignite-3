package org.apache.ignite.cli.core.flow;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.ignite.cli.commands.decorators.DefaultDecorator;
import org.apache.ignite.cli.commands.decorators.core.Decorator;
import org.apache.ignite.cli.commands.decorators.core.TerminalOutput;
import org.apache.ignite.cli.core.call.CallExecutionPipeline;
import org.apache.ignite.cli.core.call.CallInput;
import org.apache.ignite.cli.core.exception.ExceptionHandler;
import org.apache.ignite.cli.core.exception.ExceptionHandlers;
import org.apache.ignite.cli.core.exception.ExceptionWriter;
import org.apache.ignite.cli.core.exception.handler.DefaultExceptionHandlers;

public class FlowExecutionPipeline<I extends CallInput, T> {
    /**
     * Call to execute.
     */
    private final FlowElement<I, T> flow;

    /**
     * Writer for execution output.
     */
    private final PrintWriter output;

    /**
     * Writer for error execution output.
     */
    private final PrintWriter errOutput;

    /**
     * Decorator that decorates call's output.
     */
    private final Decorator<T, TerminalOutput> decorator;

    /**
     * Handlers for any exceptions.
     */
    private final ExceptionHandlers exceptionHandlers;

    /**
     * Provider for call's input.
     */
    private final Supplier<I> inputProvider;

    private FlowExecutionPipeline(FlowElement<I, T> flow,
                                  PrintWriter output,
                                  PrintWriter errOutput,
                                  ExceptionHandlers exceptionHandlers,
                                  Decorator<T, TerminalOutput> decorator,
                                  Supplier<I> inputProvider) {
        this.flow = flow;
        this.output = output;
        this.exceptionHandlers = exceptionHandlers;
        this.errOutput = errOutput;
        this.decorator = decorator;
        this.inputProvider = inputProvider;
    }

    /**
     * Builder helper method.
     *
     * @return builder for {@link CallExecutionPipeline}.
     */
    public static <I extends CallInput, T> FlowExecutionPipelineBuilder<I, T> builder() {
        return new FlowExecutionPipelineBuilder<>();
    }

    /**
     * Runs the pipeline.
     *
     * @return exit code.
     */
    public int runPipeline() {
        I callInput = inputProvider.get();

        MyFlowInterrupt flowInterrupt = new MyFlowInterrupt();
        FlowOutput<T> flowOutput;
        try {
            flowOutput = flow.call(callInput, flowInterrupt);
        } catch (FlowInterruptException e) {
            flowOutput = (FlowOutput<T>) flowInterrupt.flowOutput;
        }

        if (flowOutput.hasError()) {
            return exceptionHandlers.handleException(ExceptionWriter.fromPrintWriter(errOutput), flowOutput.errorCause());
        }

        if (flowOutput.hasResult()) {
            TerminalOutput decoratedOutput = decorator.decorate(flowOutput.body());
            output.println(decoratedOutput.toTerminalString());
        }
        return 0;
    }

    /** Builder for {@link CallExecutionPipeline}. */
    public static class FlowExecutionPipelineBuilder<I extends CallInput, T> {

        private final FlowElement<I, T> flow;

        private final ExceptionHandlers exceptionHandlers = new DefaultExceptionHandlers();

        private Supplier<I> inputProvider;

        private PrintWriter output = wrapOutputStream(System.out);

        private PrintWriter errOutput = wrapOutputStream(System.err);

        private Decorator<T, TerminalOutput> decorator = new DefaultDecorator<>();

        public FlowExecutionPipelineBuilder() {
            this(null);
        }

        public FlowExecutionPipelineBuilder(FlowElement<I, T> flow) {
            this.flow = flow;
        }


        public <OT> FlowExecutionPipelineBuilder<I, OT> flow(FlowElement<FlowOutput<T>, OT> flow) {
            return new FlowAppenderExecutionPipelineBuilder<>(this, flow);
        }

        public FlowBranchExecutionPipelineBuilder<I, T> branches() {
            return new FlowBranchExecutionPipelineBuilder<>(this);
        }

        public FlowExecutionPipelineBuilder<I, T> interruptHandler(InterruptHandler<T> interruptHandler) {
            return this;
        }

        public FlowExecutionPipelineBuilder<I, T> inputProvider(Supplier<I> inputProvider) {
            this.inputProvider = inputProvider;
            return this;
        }

        public FlowExecutionPipelineBuilder<I, T> output(PrintWriter output) {
            this.output = output;
            return this;
        }

        public FlowExecutionPipelineBuilder<I, T> output(OutputStream output) {
            return output(wrapOutputStream(output));
        }

        public FlowExecutionPipelineBuilder<I, T> errOutput(PrintWriter errOutput) {
            this.errOutput = errOutput;
            return this;
        }

        public FlowExecutionPipelineBuilder<I, T> errOutput(OutputStream output) {
            return errOutput(wrapOutputStream(output));
        }

        public FlowExecutionPipelineBuilder<I, T> exceptionHandler(ExceptionHandler<?> exceptionHandler) {
            exceptionHandlers.addExceptionHandler(exceptionHandler);
            return this;
        }

        public FlowExecutionPipelineBuilder<I, T> exceptionHandlers(ExceptionHandlers exceptionHandlers) {
            this.exceptionHandlers.addExceptionHandlers(exceptionHandlers);
            return this;
        }

        public FlowExecutionPipelineBuilder<I, T> decorator(Decorator<T, TerminalOutput> decorator) {
            this.decorator = decorator;
            return this;
        }

        public FlowExecutionPipeline<I, T> build() {
            return new FlowExecutionPipeline<>(flow, output, errOutput, exceptionHandlers, decorator, inputProvider);
        }

        private static PrintWriter wrapOutputStream(OutputStream output) {
            return new PrintWriter(output, true, getStdoutEncoding());
        }

        private static Charset getStdoutEncoding() {
            String encoding = System.getProperty("sun.stdout.encoding");
            return encoding != null ? Charset.forName(encoding) : Charset.defaultCharset();
        }
    }


    public static class FlowAppenderExecutionPipelineBuilder<I extends CallInput, T> {
        private final FlowExecutionPipelineBuilder<I, T> flowBuilder;

        public FlowAppenderExecutionPipelineBuilder(FlowExecutionPipelineBuilder<I, T> flowBuilder, FlowElement<FlowOutput<T>, OT>) {
            this.flowBuilder = flowBuilder;
        }

        public <OT> FlowExecutionPipelineBuilder<I, OT> appendFlow(FlowElement<FlowOutput<T>, OT> next) {
            return new FlowExecutionPipelineBuilder<>(flow.composite(next))
                    .inputProvider(inputProvider)
                    .output(output)
                    .errOutput(errOutput)
                    .exceptionHandlers(exceptionHandlers);
        }
    }

    public static class FlowBranchExecutionPipelineBuilder<I extends CallInput, T> {
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

    private static class MyFlowInterrupt implements FlowInterrupter {
        private FlowOutput<?> flowOutput;

        @Override
        public void interrupt(FlowOutput<?> flowOutput) {
            this.flowOutput = flowOutput;
            throw new FlowInterruptException();
        }
    }
}
