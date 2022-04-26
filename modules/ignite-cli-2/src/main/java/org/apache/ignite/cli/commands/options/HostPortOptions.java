package org.apache.ignite.cli.commands.options;

import java.net.InetAddress;
import picocli.CommandLine.Option;

public class HostPortOptions {
    @Option(names = "--host", required = true)
    InetAddress host;

    @Option(names = "--port", required = true)
    int port;
}
