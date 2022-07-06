package org.apache.ignite.cli.core.flow;

import java.util.function.Supplier;
import org.apache.ignite.cli.commands.decorators.core.Decorator;
import org.apache.ignite.cli.commands.decorators.core.TerminalOutput;

public interface Flowable<T> {

    default boolean hasResult() {
        return value() != null;
    }


    Class<T> type();

    /**
     * Body provider method.
     *
     * @return Body of the call's output. Can be {@link String} or any other type.
     */
    T value();

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

    static <O> O interrupt() {
        throw new FlowInterruptException();
    }

    static <O> Flowable<O> interrupt(O value) {
        throw new FlowInterruptException();
    }

    static <O> Flowable<O> interrupt(Throwable t) {
        throw new FlowInterruptException();
    }

    static <T> Flowable<T> process(Supplier<T> s) {
        try {
            return success(s.get());
        } catch (Exception e) {
            return failure(e);
        }
    }

    /**
     * New successful call output with provided body.
     *
     * @param body for successful call output.
     * @return Successful call output with provided body.
     */
    static <T> Flowable<T> success(T body) {
        return DefaultFlowable.<T>builder()
                .body(body)
                .build();
    }

    /**
     * New failed call output with provided cause.
     *
     * @param cause error of failed call.
     * @return Failed call output with provided cause.
     */
    static <T> Flowable<T> failure(Throwable cause) {
        return DefaultFlowable.<T>builder()
                .cause(cause)
                .build();
    }

    /**
     * New empty coll output.
     *
     * @return Empty call output.
     */
    static <T> Flowable<T> empty() {
        return DefaultFlowable.<T>builder()
                .build();
    }
}
