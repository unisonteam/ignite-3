package org.apache.ignite.cli.core.flow;

import java.util.function.Supplier;

/**
 * Element of {@link Flow} processing.
 *
 * @param <T> result type.
 */
public interface Flowable<T> {

    /**
     * Result check method.
     *
     * @return true if flowable has result.
     */
    default boolean hasResult() {
        return value() != null;
    }


    Class<T> type();

    /**
     * Value provider method.
     *
     * @return Value of the flowable's output.
     */
    T value();

    /**
     * Error check method.
     *
     * @return True if flowable has an error.
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

    /**
     * Create interrupted flowable.
     *
     * @param <O> required type.
     * @return nothing.
     */
    static <O> O interrupt() {
        throw new FlowInterruptException();
    }

    /**
     * Transform supplier to Flowable with result.
     *
     * @param supplier value supplier.
     * @param <T> value type.
     * @return {@link Flowable} with supplier result or any exception.
     */
    static <T> Flowable<T> process(Supplier<T> supplier) {
        try {
            return success(supplier.get());
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
