package org.apache.ignite.cli.core.flow;

import java.util.Objects;
import java.util.StringJoiner;
import org.apache.ignite.cli.core.call.DefaultCallOutput;

public class DefaultFlowOutput<T> implements FlowOutput<T> {
    private final T body;

    private final Throwable cause;

    private DefaultFlowOutput(T body, Throwable cause) {
        this.body = body;
        this.cause = cause;
    }

    @Override
    public boolean hasError() {
        return cause != null;
    }

    @Override
    public Throwable errorCause() {
        return cause;
    }

    @Override
    public T body() {
        return body;
    }

    @Override
    public boolean hasResult() {
        return body != null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DefaultFlowOutput<?> that = (DefaultFlowOutput<?>) o;
        return Objects.equals(body, that.body) && Objects.equals(cause, that.cause);
    }

    @Override
    public int hashCode() {
        return Objects.hash(body, cause);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", DefaultFlowOutput.class.getSimpleName() + "[", "]")
                .add("body=" + body)
                .add("cause=" + cause)
                .toString();
    }

    /**
     * Builder method provider.
     *
     * @return new instance of {@link DefaultCallOutput.DefaultCallOutputBuilder}.
     */
    public static <T> DefaultFlowOutputBuilder<T> builder() {
        return new DefaultFlowOutputBuilder<>();
    }

    /**
     * New successful call output with provided body.
     *
     * @param body for successful call output.
     * @return Successful call output with provided body.
     */
    public static <T> DefaultFlowOutput<T> success(T body) {
        return DefaultFlowOutput.<T>builder()
                .body(body)
                .build();
    }

    /**
     * New failed call output with provided cause.
     *
     * @param cause error of failed call.
     * @return Failed call output with provided cause.
     */
    public static <T> DefaultFlowOutput<T> failure(Throwable cause) {
        return DefaultFlowOutput.<T>builder()
                .cause(cause)
                .build();
    }

    /**
     * New empty coll output.
     *
     * @return Empty call output.
     */
    public static <T> DefaultFlowOutput<T> empty() {
        return DefaultFlowOutput.<T>builder()
                .build();
    }

    /**
     * Builder of {@link DefaultCallOutput}.
     */
    public static class DefaultFlowOutputBuilder<T> {

        private T body;

        private Throwable cause;

        /**
         * Builder setter.
         *
         * @param body call output body.
         * @return invoked builder instance {@link DefaultCallOutput.DefaultCallOutputBuilder}.
         */
        public DefaultFlowOutputBuilder<T> body(T body) {
            this.body = body;
            return this;
        }

        /**
         * Builder setter.
         *
         * @param cause exception cause.
         * @return invoked builder instance {@link DefaultCallOutput.DefaultCallOutputBuilder}.
         */
        public DefaultFlowOutputBuilder<T> cause(Throwable cause) {
            this.cause = cause;
            return this;
        }

        /**
         * Build method.
         *
         * @return new {@link DefaultCallOutput} with field provided to builder.
         */
        public DefaultFlowOutput<T> build() {
            return new DefaultFlowOutput<>(body, cause);
        }
    }
}
