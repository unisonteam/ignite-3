package org.apache.ignite.cli.core.call;

/**
 * Output of {@link Call}.
 * @param <T> type of the body.
 */
public interface CallOutput<T> {
    /**
     * @return Body of the call's output. Can be {@link String} or any other type.
     */
    T body();

    /**
     * @return Status of the call execution.
     */
    CallOutputStatus status();

    /**
     * @return True if output has an error.
     */
    boolean hasError();

    /**
     * @return the cause of the error.
     */
    Throwable errorCause();
}
