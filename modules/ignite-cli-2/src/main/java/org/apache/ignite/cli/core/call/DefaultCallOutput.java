package org.apache.ignite.cli.core.call;

import java.util.Objects;

/**
 * Default implementation of {@link CallOutput} with {@link String} body.
 */
public class DefaultCallOutput implements CallOutput<String> {
    private final CallOutputStatus status;
    private final String body;
    private final Throwable cause;

    private DefaultCallOutput(CallOutputStatus status, String body, Throwable cause) {
        this.status = status;
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
    public String body() {
        return body;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DefaultCallOutput that = (DefaultCallOutput) o;
        return status == that.status && Objects.equals(body, that.body) && Objects.equals(cause, that.cause);
    }

    @Override
    public int hashCode() {
        return Objects.hash(status, body, cause);
    }

    @Override
    public String toString() {
        return "DefaultCallOutput{"
            + "status="
            + status
            + ", body='"
            + body + '\''
            + ", cause="
            + cause
            + '}';
    }

    /**
     * Builder method provider.
     *
     * @return new instance of {@link DefaultCallOutputBuilder}.
     */
    public static DefaultCallOutputBuilder builder() {
        return new DefaultCallOutputBuilder();
    }

    /**
     * New successful call output with provided body.
     *
     * @param body for successful call output.
     * @return Successful call output with provided body.
     */
    public static DefaultCallOutput success(String body) {
        return DefaultCallOutput.builder()
                .status(CallOutputStatus.SUCCESS)
                .body(body)
                .build();
    }

    /**
     * New failed call output with provided cause.
     *
     * @param cause error of failed call.
     * @return Failed call output with provided cause.
     */
    public static DefaultCallOutput failure(Throwable cause) {
        return DefaultCallOutput.builder()
            .status(CallOutputStatus.ERROR)
            .cause(cause)
            .build();
    }

    /**
     * Builder of {@link DefaultCallOutput}.
     */
    public static class DefaultCallOutputBuilder {
        private CallOutputStatus status;
        private String body;
        private Throwable cause;

        /**
         * Builder setter.
         *
         * @param status output status.
         * @return invoked builder instance {@link DefaultCallOutputBuilder}.
         */
        public DefaultCallOutputBuilder status(CallOutputStatus status) {
            this.status = status;
            return this;
        }

        /**
         * Builder setter.
         *
         * @param body call output body.
         * @return invoked builder instance {@link DefaultCallOutputBuilder}.
         */
        public DefaultCallOutputBuilder body(String body) {
            this.body = body;
            return this;
        }

        /**
         * Builder setter.
         *
         * @param cause exception cause.
         * @return invoked builder instance {@link DefaultCallOutputBuilder}.
         */
        public DefaultCallOutputBuilder cause(Throwable cause) {
            this.cause = cause;
            return this;
        }

        /**
         * Build method.
         *
         * @return new {@link DefaultCallOutput} with field provided to builder.
         */
        public DefaultCallOutput build() {
            return new DefaultCallOutput(status, body, cause);
        }
    }
}
