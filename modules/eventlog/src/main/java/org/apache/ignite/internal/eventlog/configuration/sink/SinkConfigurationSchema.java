package org.apache.ignite.internal.eventlog.configuration.sink;

import org.apache.ignite.configuration.annotation.InjectedName;
import org.apache.ignite.configuration.annotation.PolymorphicConfig;
import org.apache.ignite.configuration.annotation.PolymorphicId;
import org.apache.ignite.configuration.annotation.Value;

@PolymorphicConfig
public class SinkConfigurationSchema {
    @PolymorphicId
    public String type;

    @InjectedName
    public String name;

    @Value(hasDefault = true)
    public String[] channels = new String[0];
}
