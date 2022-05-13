package org.apache.ignite.cli.core.call;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.function.Supplier;
import org.apache.ignite.cli.commands.decorators.DefaultDecorator;
import org.apache.ignite.cli.commands.decorators.core.Decorator;
import org.apache.ignite.cli.commands.decorators.core.TerminalOutput;

/**
 * Implementation of {@link CallExecutionPipeline} that is used by default.
 *
 * @param <I> Call input type.
 * @param <T> Call output's body type.
 */
public class DefaultCallExecutionPipeline<I extends CallInput, T> {
    /**
     * Call to execute.
     */
    private final Call<I, T> call;
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
     * Provider for call's input.
     */
    private final Supplier<I> inputProvider;

    private DefaultCallExecutionPipeline(Call<I, T> call, PrintWriter output,
            PrintWriter errOutput, Decorator<T, TerminalOutput> decorator, Supplier<I> inputProvider) {
        this.call = call;
        this.output = output;
        this.errOutput = errOutput;
        this.decorator = decorator;
        this.inputProvider = inputProvider;
    }

    /**
     * Builder helper method.
     *
     * @return builder for {@link DefaultCallExecutionPipeline}.
     * */
    public static <I extends CallInput, T> DefaultCommandExecutionPipelineBuilder<I, T> builder(
            Call<I, T> call) {
        return new DefaultCommandExecutionPipelineBuilder<>(call);
    }

    /** {@inheritDoc} */
    public void runPipeline() {
        I callInput = inputProvider.get();

        CallOutput<T> callOutput = call.execute(callInput);

        if (callOutput.hasError()) {
            errOutput.println("Got error during command execution: " + callOutput.errorCause()); // fixme
            return;
        }

        TerminalOutput decoratedOutput = decorator.decorate(callOutput.body());

        output.println(decoratedOutput.toTerminalString());
    }

    /** Builder for {@link DefaultCallExecutionPipeline}. */
    public static class DefaultCommandExecutionPipelineBuilder<I extends CallInput, T> {

        private Supplier<I> inputProvider;
        private Call<I, T> call;
        private PrintWriter output = new PrintWriter(System.out);
        private PrintWriter errOutput = new PrintWriter(System.err);
        private Decorator<T, TerminalOutput> decorator = new DefaultDecorator<>();

        public DefaultCommandExecutionPipelineBuilder(Call<I, T> call) {
            this.call = call;
        }

        public DefaultCommandExecutionPipelineBuilder<I, T> inputProvider(Supplier<I> inputProvider) {
            this.inputProvider = inputProvider;
            return this;
        }

        public DefaultCommandExecutionPipelineBuilder<I, T> output(PrintWriter output) {
            this.output = output;
            return this;
        }

        public DefaultCommandExecutionPipelineBuilder<I, T> output(OutputStream output) {
            return output(wrapOutputStream(output));
        }

        public DefaultCommandExecutionPipelineBuilder<I, T> errOutput(PrintWriter errOutput) {
            this.errOutput = errOutput;
            return this;
        }

        public DefaultCommandExecutionPipelineBuilder<I, T> errOutput(OutputStream output) {
            return errOutput(wrapOutputStream(output));
        }

        public DefaultCommandExecutionPipelineBuilder<I, T> decorator(Decorator<T, TerminalOutput> decorator) {
            this.decorator = decorator;
            return this;
        }

        public DefaultCallExecutionPipeline<I, T> build() {
            return new DefaultCallExecutionPipeline<>(call, output, errOutput, decorator, inputProvider);
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
