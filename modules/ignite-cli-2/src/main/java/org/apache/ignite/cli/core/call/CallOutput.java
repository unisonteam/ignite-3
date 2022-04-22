package org.apache.ignite.cli.core.call;

/**
 * Output of {@link Call}.
 *
 * @param <T> type of the body.
 */
public interface CallOutput<T> {
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
    boolean hasError();

    /**
     * Exception cause provider method.
     *
     * @return the cause of the error.
     */
    Throwable errorCause();
}
