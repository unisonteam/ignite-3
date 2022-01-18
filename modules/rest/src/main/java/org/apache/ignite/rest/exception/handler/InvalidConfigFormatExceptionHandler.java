package org.apache.ignite.rest.exception.handler;

import com.google.gson.Gson;
import io.micronaut.context.annotation.Requires;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.server.exceptions.ExceptionHandler;
import jakarta.inject.Singleton;
import org.apache.ignite.rest.ErrorResult;
import org.apache.ignite.rest.exception.InvalidConfigFormatException;

@Produces
@Singleton
@Requires(classes = {InvalidConfigFormatException.class, ExceptionHandler.class})
public class InvalidConfigFormatExceptionHandler implements ExceptionHandler<InvalidConfigFormatException, HttpResponse<String>> {
    
    @Override
    public HttpResponse<String> handle(HttpRequest request, InvalidConfigFormatException exception) {
        final ErrorResult errorResult = new ErrorResult("INVALID_CONFIG_FORMAT", exception.getMessage());
        // TODO: IGNITE-14344 Gson object should not be created on every response
        var body = new Gson().toJson(errorResult);
        
        return HttpResponse.badRequest().body(body);
    }
}