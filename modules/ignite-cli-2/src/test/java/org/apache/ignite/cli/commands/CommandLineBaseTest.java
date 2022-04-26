package org.apache.ignite.cli.commands;

import io.micronaut.configuration.picocli.MicronautFactory;
import io.micronaut.context.ApplicationContext;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.io.PrintWriter;
import java.io.StringWriter;
import picocli.CommandLine;

@MicronautTest
public class CommandLineBaseTest {

    protected CommandLine commandLine;
    protected StringWriter err;
    protected StringWriter out;
    @Inject
    ApplicationContext context;

    protected void setUp(Class<?> commandClass) {
        err = new StringWriter();
        out = new StringWriter();
        commandLine = new CommandLine(commandClass, new MicronautFactory(context));
        commandLine.setErr(new PrintWriter(err));
        commandLine.setOut(new PrintWriter(out));
    }

}