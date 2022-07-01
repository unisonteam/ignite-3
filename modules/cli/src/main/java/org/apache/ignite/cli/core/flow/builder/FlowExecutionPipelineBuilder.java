package org.apache.ignite.cli.core.flow.builder;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.function.Supplier;
import org.apache.ignite.cli.core.call.CallExecutionPipeline;
import org.apache.ignite.cli.core.call.CallInput;
import org.apache.ignite.cli.core.exception.ExceptionHandler;
import org.apache.ignite.cli.core.exception.ExceptionHandlers;
import org.apache.ignite.cli.core.exception.handler.DefaultExceptionHandlers;
import org.apache.ignite.cli.core.flow.FlowElement;
import org.apache.ignite.cli.core.flow.FlowOutput;

/**
 * Builder for {@link CallExecutionPipeline}.
 */
public class FlowExecutionPipelineBuilder<I extends CallInput, O> {

    protected final FlowElement<I, O> flow;

    protected final ExceptionHandlers exceptionHandlers = new DefaultExceptionHandlers();

    protected Supplier<I> inputProvider;

    protected PrintWriter output = wrapOutputStream(System.out);

    protected PrintWriter errOutput = wrapOutputStream(System.err);


    public FlowExecutionPipelineBuilder(FlowElement<I, O> flow) {
        this.flow = flow;
    }


    public <OT> UnhandledFlowExecutionPipelineBuilder<I, OT> appendFlow(FlowElement<FlowOutput<O>, OT> next) {
        return new UnhandledFlowExecutionPipelineBuilder<>(flow.composite(next))
                .inputProvider(inputProvider)
                .output(output)
                .errOutput(errOutput)
                .exceptionHandlers(exceptionHandlers);
    }



    public FlowBranchExecutionPipelineBuilder<I, O> branches() {
        return new FlowBranchExecutionPipelineBuilder<>(this);
    }

    public FlowExecutionPipelineBuilder<I, O> inputProvider(Supplier<I> inputProvider) {
        this.inputProvider = inputProvider;
        return this;
    }

    public FlowExecutionPipelineBuilder<I, O> output(PrintWriter output) {
        this.output = output;
        return this;
    }

    public FlowExecutionPipelineBuilder<I, O> output(OutputStream output) {
        return output(wrapOutputStream(output));
    }

    public FlowExecutionPipelineBuilder<I, O> errOutput(PrintWriter errOutput) {
        this.errOutput = errOutput;
        return this;
    }

    public FlowExecutionPipelineBuilder<I, O> errOutput(OutputStream output) {
        return errOutput(wrapOutputStream(output));
    }

    public FlowExecutionPipelineBuilder<I, O> exceptionHandler(ExceptionHandler<?> exceptionHandler) {
        exceptionHandlers.addExceptionHandler(exceptionHandler);
        return this;
    }

    public FlowExecutionPipelineBuilder<I, O> exceptionHandlers(ExceptionHandlers exceptionHandlers) {
        this.exceptionHandlers.addExceptionHandlers(exceptionHandlers);
        return this;
    }

    public FlowExecutionPipeline<I, O> build() {
        return new FlowExecutionPipeline<>(flow, output, errOutput, exceptionHandlers, inputProvider);
    }

    private static PrintWriter wrapOutputStream(OutputStream output) {
        return new PrintWriter(output, true, getStdoutEncoding());
    }

    private static Charset getStdoutEncoding() {
        String encoding = System.getProperty("sun.stdout.encoding");
        return encoding != null ? Charset.forName(encoding) : Charset.defaultCharset();
    }
}
