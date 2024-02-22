package org.apache.ignite.internal.eventlog.configuration.channel;

import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.InjectedName;
import org.apache.ignite.configuration.annotation.Value;

@Config
public class ChannelConfigurationSchema {
    @InjectedName
    public String name;

    @Value(hasDefault = true)
    public boolean enabled = false;
}
