package org.apache.ignite.cli.commands.options;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

public class ClusterConnectivityOptions {
    @Option(names = "--cluster-url")
    String url;
    @ArgGroup(exclusive = false)
    HostPortOptions hostPortOptions;

    public String getUrl() {
        return url;
    }

    public HostPortOptions getHostPortOptions() {
        return hostPortOptions;
    }
}
