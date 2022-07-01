package org.apache.ignite.cli.core.flow.builder;

import java.io.PrintWriter;
import java.util.function.Supplier;
import org.apache.ignite.cli.commands.decorators.core.TerminalOutput;
import org.apache.ignite.cli.core.DecoratorRegistry;
import org.apache.ignite.cli.core.call.CallExecutionPipeline;
import org.apache.ignite.cli.core.call.CallInput;
import org.apache.ignite.cli.core.exception.ExceptionHandlers;
import org.apache.ignite.cli.core.exception.ExceptionWriter;
import org.apache.ignite.cli.core.flow.DefaultDecoratorRegistry;
import org.apache.ignite.cli.core.flow.DefaultFlowOutput;
import org.apache.ignite.cli.core.flow.FlowElement;
import org.apache.ignite.cli.core.flow.FlowInterruptException;
import org.apache.ignite.cli.core.flow.FlowInterrupter;
import org.apache.ignite.cli.core.flow.FlowOutput;
import org.apache.ignite.cli.core.flow.builder.FlowExecutionPipelineBuilder;
import org.apache.ignite.cli.core.flow.builder.UnhandledFlowExecutionPipelineBuilder;

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
    private final DecoratorRegistry store = new DefaultDecoratorRegistry();

    /**
     * Handlers for any exceptions.
     */
    private final ExceptionHandlers exceptionHandlers;

    /**
     * Provider for call's input.
     */
    private final Supplier<I> inputProvider;

    FlowExecutionPipeline(FlowElement<I, T> flow,
                                  PrintWriter output,
                                  PrintWriter errOutput,
                                  ExceptionHandlers exceptionHandlers,
                                  Supplier<I> inputProvider) {
        this.flow = flow;
        this.output = output;
        this.exceptionHandlers = exceptionHandlers;
        this.errOutput = errOutput;
        this.inputProvider = inputProvider;
    }

    /**
     * Builder helper method.
     *
     * @return builder for {@link CallExecutionPipeline}.
     */
    public static <I extends CallInput, T> UnhandledFlowExecutionPipelineBuilder<I, T> builder(FlowElement<I, T> flowElement) {
        return new UnhandledFlowExecutionPipelineBuilder<>(flowElement);
    }

    /**
     * Runs the pipeline.
     *
     * @return exit code.
     */
    public int runPipeline() {
        I callInput = inputProvider.get();

        FlowInterrupter flowInterrupt = flowOutput -> {
            throw new FlowInterruptException();
        };
        FlowOutput<T> flowOutput;
        try {
            flowOutput = flow.call(callInput, flowInterrupt);
        } catch (FlowInterruptException e) {
            flowOutput = DefaultFlowOutput.empty();
        }

        if (flowOutput.hasError()) {
            return exceptionHandlers.handleException(ExceptionWriter.fromPrintWriter(errOutput), flowOutput.errorCause());
        }

        if (flowOutput.hasResult()) {
            TerminalOutput decoratedOutput = store.getDecorator(flowOutput.type()).decorate(flowOutput.body());
            output.println(decoratedOutput.toTerminalString());
        }
        return 0;
    }
}
