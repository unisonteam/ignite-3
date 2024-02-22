package org.apache.ignite.internal.eventlog.configuration.sink;

import org.apache.ignite.configuration.annotation.PolymorphicConfigInstance;
import org.apache.ignite.configuration.annotation.Value;

@PolymorphicConfigInstance("file")
public class FileSinkConfigurationSchema extends SinkConfigurationSchema {
    @Value(hasDefault = true)
    public final String pattern = "apache-ignite-event.log";
}
