package org.apache.ignite.rest.exception;

/**
 * Exception that is thrown when the wrong configuration path is given.
 */
public class ConfigPathUnrecognizedException extends RuntimeException {
    public ConfigPathUnrecognizedException(Throwable cause) {
        super(cause);
    }
}
