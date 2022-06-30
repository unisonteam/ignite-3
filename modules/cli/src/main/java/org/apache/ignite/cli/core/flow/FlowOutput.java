package org.apache.ignite.cli.core.flow;

import org.apache.ignite.cli.commands.decorators.core.Decorator;
import org.apache.ignite.cli.commands.decorators.core.TerminalOutput;

public interface FlowOutput<T> {

    default boolean hasResult() {
        return body() != null;
    }


    Class<T> type();

    /**
     * Body provider method.
     *
     * @return Body of the call's output. Can be {@link String} or any other type.
     */
    T body();

    /**
     * Error check method.
     *
     * @return True if output has an error.
     */
    default boolean hasError() {
        return errorCause() != null;
    }

    /**
     * Exception cause provider method.
     *
     * @return the cause of the error.
     */
    Throwable errorCause();



    default FlowOutput<String> transform(Decorator<T, TerminalOutput> decorator) {
        return new FlowOutput<>() {
            @Override
            public boolean hasResult() {
                return FlowOutput.super.hasResult();
            }

            @Override
            public Class<String> type() {
                return String.class;
            }

            @Override
            public String body() {
                return decorator.decorate(FlowOutput.this.body()).toTerminalString();
            }

            @Override
            public Throwable errorCause() {
                return FlowOutput.this.errorCause();
            }
        };
    }
}
