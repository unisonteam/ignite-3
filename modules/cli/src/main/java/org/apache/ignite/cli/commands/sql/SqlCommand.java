package org.apache.ignite.cli.commands.sql;

import jakarta.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.SQLException;
import org.apache.ignite.cli.commands.BaseCommand;
import org.apache.ignite.cli.commands.decorators.SqlQueryResultDecorator;
import org.apache.ignite.cli.core.CallExecutionPipelineProvider;
import org.apache.ignite.cli.core.call.CallExecutionPipeline;
import org.apache.ignite.cli.core.call.StringCallInput;
import org.apache.ignite.cli.core.exception.CommandExecutionException;
import org.apache.ignite.cli.core.exception.ExceptionWriter;
import org.apache.ignite.cli.core.exception.handler.DefaultExceptionHandlers;
import org.apache.ignite.cli.core.exception.handler.SqlExceptionHandler;
import org.apache.ignite.cli.core.repl.Repl;
import org.apache.ignite.cli.core.repl.executor.RegistryCommandExecutor;
import org.apache.ignite.cli.core.repl.executor.ReplExecutor;
import org.apache.ignite.cli.core.repl.executor.SqlQueryCall;
import org.apache.ignite.cli.sql.SqlManager;
import org.apache.ignite.cli.sql.SqlSchemaProvider;
import org.jetbrains.annotations.NotNull;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Command for sql execution.
 */
@Command(name = "sql", description = "Executes SQL query.")
public class SqlCommand extends BaseCommand implements Runnable {
    private static final String INTERNAL_COMMAND_PREFIX = "!";

    @Option(names = {"-u", "--jdbc-url"}, required = true,
            descriptionKey = "ignite.jdbc-url", description = "JDBC url to ignite cluster")
    private String jdbc;
    @Option(names = {"-e", "--execute", "--exec"}) //todo: can be passed as parameter, not option (see IEP-88)
    private String command;
    @Option(names = {"-f", "--script-file"})
    private File file;

    @Inject
    private ReplExecutor replExecutor;

    private static String extract(File file) {
        try {
            return String.join("\n", Files.readAllLines(file.toPath(), StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new CommandExecutionException("sql", "File with command not found.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void run() {
        try (SqlManager sqlManager = new SqlManager(jdbc)) {
            if (command == null && file == null) {
                replExecutor.execute(Repl.builder()
                        .withPromptProvider(() -> "sql-cli> ")
                        .withCompleter(new SqlCompleter(new SqlSchemaProvider(sqlManager::getMetadata)))
                        .withCommandClass(SqlReplTopLevelCliCommand.class)
                        .withCallExecutionPipelineBuilderProvider(provider(sqlManager))
                        .build());
            } else {
                String executeCommand = file != null ? extract(file) : command;
                createSqlExecPipeline(sqlManager, executeCommand).runPipeline();
            }
        } catch (SQLException e) {
            new SqlExceptionHandler().handle(ExceptionWriter.fromPrintWriter(spec.commandLine().getErr()), e);
        }
    }

    @NotNull
    private CallExecutionPipelineProvider provider(SqlManager sqlManager) {
        return (call, line) -> line.startsWith(INTERNAL_COMMAND_PREFIX)
                ? createInternalCommandPipeline(call, line)
                : createSqlExecPipeline(sqlManager, line);
    }

    private CallExecutionPipeline<?, ?> createSqlExecPipeline(SqlManager sqlManager, String line) {
        return CallExecutionPipeline.builder(new SqlQueryCall(sqlManager))
                .inputProvider(() -> new StringCallInput(line))
                .output(spec.commandLine().getOut())
                .errOutput(spec.commandLine().getErr())
                .exceptionHandlers(new DefaultExceptionHandlers())
                .decorator(new SqlQueryResultDecorator())
                .build();
    }

    private CallExecutionPipeline<?, ?> createInternalCommandPipeline(RegistryCommandExecutor call, String line) {
        return CallExecutionPipeline.builder(call)
                .inputProvider(() -> new StringCallInput(line.substring(INTERNAL_COMMAND_PREFIX.length())))
                .output(System.out)
                .errOutput(System.err)
                .exceptionHandlers(new DefaultExceptionHandlers())
                .build();
    }
}
