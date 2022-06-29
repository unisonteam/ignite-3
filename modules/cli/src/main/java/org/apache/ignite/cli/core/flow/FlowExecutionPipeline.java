package org.apache.ignite.cli.core.flow;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.charset.Charset;
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
    public static <I extends CallInput, T> FlowExecutionPipelineBuilder<I, T> builder(FlowElement<I, T> flow) {
        return new FlowExecutionPipelineBuilder<>(flow);
    }

    /**
     * Runs the pipeline.
     *
     * @return exit code.
     */
    public int runPipeline() {
        I callInput = inputProvider.get();

        FlowOutput<T> flowOutput = null;
        try {
            flowOutput = flow.call(callInput);
        } catch (FlowInterruptException e) {
            flowOutput = DefaultFlowOutput.failure(e.getCause());
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

        public FlowExecutionPipelineBuilder(FlowElement<I, T> flow) {
            this.flow = flow;
        }

        public <OT> FlowExecutionPipelineBuilder<I, OT> appendFlow(FlowElement<FlowOutput<T>, OT> next, FlowInterrupter<T> interrupter) {
            return new FlowExecutionPipelineBuilder<>(flow.composite(next, interrupter))
                    .inputProvider(inputProvider)
                    .output(output)
                    .errOutput(errOutput)
                    .exceptionHandlers(exceptionHandlers);
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
}
