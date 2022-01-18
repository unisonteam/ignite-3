package org.apache.ignite.rest;

import io.micronaut.context.annotation.BootstrapContextCompatible;
import io.micronaut.context.annotation.Context;
import io.micronaut.context.annotation.Factory;
import io.netty.channel.EventLoopGroup;
import jakarta.inject.Named;
import jakarta.inject.Qualifier;
import jakarta.inject.Singleton;
import org.apache.ignite.internal.network.netty.NamedNioEventLoopGroup;

@Factory
@BootstrapContextCompatible
public class NamedEventLoopGroupFactory {
    
    @Context
    @Named("ignite-worker")
    public EventLoopGroup igniteWorker() {
        return NamedNioEventLoopGroup.create("ignite-worker");
    }
}
