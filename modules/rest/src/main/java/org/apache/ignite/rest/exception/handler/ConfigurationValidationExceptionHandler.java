package org.apache.ignite.rest.exception.handler;

import io.micronaut.context.annotation.Requires;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.server.exceptions.ExceptionHandler;
import jakarta.inject.Singleton;
import org.apache.ignite.configuration.validation.ConfigurationValidationException;
import org.apache.ignite.rest.ErrorResult;

/**
 * Handles {@link ConfigurationValidationException} and represents it as a rest response.
 */
@Produces
@Singleton
@Requires(classes = {ConfigurationValidationException.class, ExceptionHandler.class})
public class ConfigurationValidationExceptionHandler implements
        ExceptionHandler<ConfigurationValidationException, HttpResponse<ErrorResult>> {

    @Override
    public HttpResponse<ErrorResult> handle(HttpRequest request, ConfigurationValidationException exception) {
        final ErrorResult errorResult = new ErrorResult("VALIDATION_EXCEPTION", exception.getMessage());
        return HttpResponse.badRequest().body(errorResult);
    }
}
