package org.apache.ignite.cli.commands.sql;

import jakarta.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import org.apache.ignite.cli.commands.BaseCommand;
import org.apache.ignite.cli.commands.decorators.SqlQueryResultDecorator;
import org.apache.ignite.cli.core.call.CallExecutionPipeline;
import org.apache.ignite.cli.core.call.StringCallInput;
import org.apache.ignite.cli.core.repl.executor.SqlQueryCall;
import org.apache.ignite.cli.sql.SqlManager;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

/**
 * Command for sql execution.
 */
@Command(name = "sql", description = "Executes SQL query.")
public class SqlCommand extends BaseCommand implements Runnable {
    @Option(names = {
            "--jdbc-url"}, required = true, descriptionKey = "ignite.jdbc-url", description = "JDBC url to ignite cluster")
    private String jdbc;
    @Option(names = {"-e", "--execute"}) //todo: can be passed as parameter, not option (see IEP-88)
    private String command;
    @Option(names = {"--script-file"})
    private File file;

    @Spec
    private CommandSpec spec;

    @Inject
    private SqlReplExecutor sqlReplExecutor;

    private static String extract(File file) throws IOException {
        return String.join("\n", Files.readAllLines(file.toPath(), StandardCharsets.UTF_8));
    }

    /** {@inheritDoc} */
    @Override
    public void run() {
        try (SqlManager sqlManager = new SqlManager(jdbc)) {
            if (command == null && file == null) {
                sqlReplExecutor.executeRepl(sqlManager);
            } else {
                String executeCommand = file != null ? extract(file) : command;
                SqlQueryCall call = new SqlQueryCall(sqlManager);
                CallExecutionPipeline.builder(call).inputProvider(() -> new StringCallInput(executeCommand))
                        .output(spec.commandLine().getOut()).errOutput(spec.commandLine().getErr()).decorator(new SqlQueryResultDecorator())
                        .build().runPipeline();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
