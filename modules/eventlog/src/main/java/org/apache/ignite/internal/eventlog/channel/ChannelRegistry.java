package org.apache.ignite.internal.eventlog.channel;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.configuration.NamedConfigurationTree;
import org.apache.ignite.configuration.notifications.ConfigurationNamedListListener;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.internal.eventlog.configuration.channel.ChannelChange;
import org.apache.ignite.internal.eventlog.configuration.channel.ChannelConfiguration;
import org.apache.ignite.internal.eventlog.configuration.channel.ChannelView;

public class ChannelRegistry {
    private final Map<String, Channel> channelsByName;

    public ChannelRegistry(NamedConfigurationTree<ChannelConfiguration, ChannelView, ChannelChange> channels) {
        channels.listenElements(new ChannelConfigurationUpdateListener());
        channelsByName = new HashMap<>();
    }

    public Collection<Channel> allChannels() {
        return channelsByName.values();
    }

    public Channel findByName(String testChannelName) {
        return channelsByName.get(testChannelName);
    }

    private class ChannelConfigurationUpdateListener implements ConfigurationNamedListListener<ChannelView> {
        @Override
        public CompletableFuture<?> onCreate(ConfigurationNotificationEvent<ChannelView> ctx) {
            ChannelView channelView = ctx.newValue();
            Channel channel = new Channel(channelView.name());
            channelsByName.put(channelView.name(), channel);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<?> onDelete(ConfigurationNotificationEvent<ChannelView> ctx) {
            channelsByName.remove(ctx.oldValue().name());
            return CompletableFuture.completedFuture(null);
        }
    }
}
