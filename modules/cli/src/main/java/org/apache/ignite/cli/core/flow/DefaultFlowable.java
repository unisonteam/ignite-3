package org.apache.ignite.cli.core.flow;

import java.util.Objects;
import java.util.StringJoiner;
import org.apache.ignite.cli.core.call.DefaultCallOutput;

public class DefaultFlowable<T> implements Flowable<T> {
    private final T body;

    private final Throwable cause;

    DefaultFlowable(T body, Throwable cause) {
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
    public T value() {
        return body;
    }

    @Override
    public boolean hasResult() {
        return body != null;
    }

    @Override
    public Class<T> type() {
        return (Class<T>) body.getClass();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DefaultFlowable<?> that = (DefaultFlowable<?>) o;
        return Objects.equals(body, that.body) && Objects.equals(cause, that.cause);
    }

    @Override
    public int hashCode() {
        return Objects.hash(body, cause);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", DefaultFlowable.class.getSimpleName() + "[", "]")
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
     * Builder of {@link DefaultCallOutput}.
     */
    public static class DefaultFlowOutputBuilder<T> {

        private Class<T> type;

        private T body;

        private Throwable cause;

        /**
         * Builder setter.
         *
         * @param body call output body.
         * @return invoked builder instance {@link DefaultCallOutput.DefaultCallOutputBuilder}.
         */
        @SuppressWarnings("unchecked")
        public DefaultFlowOutputBuilder<T> body(T body) {
            this.body = body;
            type = body == null ? null : (Class<T>) body.getClass();
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
        public DefaultFlowable<T> build() {
            return new DefaultFlowable<>(body, cause);
        }
    }
}
