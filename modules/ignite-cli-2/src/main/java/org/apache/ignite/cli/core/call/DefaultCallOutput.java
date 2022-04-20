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
        return "DefaultCallOutput{" +
                "status=" + status +
                ", body='" + body + '\'' +
                ", cause=" + cause +
                '}';
    }

    public static DefaultCallOutputBuilder builder() {
        return new DefaultCallOutputBuilder();
    }

    public static DefaultCallOutput success(String body) {
        return DefaultCallOutput.builder()
                .status(CallOutputStatus.SUCCESS)
                .body(body)
                .build();
    }

    public static class DefaultCallOutputBuilder {
        private CallOutputStatus status;
        private String body;
        private Throwable cause;

        public DefaultCallOutputBuilder status(CallOutputStatus status) {
            this.status = status;
            return this;
        }

        public DefaultCallOutputBuilder body(String body) {
            this.body = body;
            return this;
        }

        public DefaultCallOutputBuilder cause(Throwable cause) {
            this.cause = cause;
            return this;
        }

        public DefaultCallOutput build() {
            return new DefaultCallOutput(status, body, cause);
        }
    }
}
