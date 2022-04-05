package org.apache.ignite.internal.rest;

import io.micronaut.context.annotation.Factory;
import io.micronaut.serde.annotation.SerdeImport;
import org.apache.ignite.internal.rest.api.ErrorResult;

@Factory
@SerdeImport(ErrorResult.class)
public class RestFactory {
}
