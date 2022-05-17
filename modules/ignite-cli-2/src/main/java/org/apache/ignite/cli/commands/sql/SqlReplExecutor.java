package org.apache.ignite.cli.commands.sql;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.ignite.cli.call.configuration.ReplCallInput;
import org.apache.ignite.cli.commands.decorators.SqlQueryResultDecorator;
import org.apache.ignite.cli.core.call.DefaultCallExecutionPipeline;
import org.apache.ignite.cli.core.repl.executor.RegistryCommandExecutor;
import org.apache.ignite.cli.core.repl.executor.ReplExecutor;
import org.apache.ignite.cli.core.repl.executor.SqlQueryCall;
import org.apache.ignite.cli.core.repl.expander.NoopExpander;
import org.apache.ignite.cli.sql.SqlManager;
import org.jline.console.impl.SystemRegistryImpl;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReader.SuggestionType;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.MaskingCallback;
import org.jline.reader.UserInterruptException;
import org.jline.reader.impl.DefaultParser;
import org.jline.terminal.Terminal;
import picocli.shell.jline3.PicocliCommands;

/**
 * SQL REPL command executor.
 */
@Singleton
public class SqlReplExecutor {
    private static final String PROMPT = "sql-cli> ";
    private static final String INTERNAL_COMMAND_PREFIX = "!";

    @Inject
    private ReplExecutor replExecutor;
    @Inject
    private Terminal terminal;

    /**
     * Execute SQL REPL method.
     *
     * @param sqlManager SQL queries executor.
     */
    public void executeRepl(SqlManager sqlManager) {
        SqlCompleter completer = new SqlCompleter(sqlManager.getSqlSchemaProvider());
        LineReader reader = LineReaderBuilder.builder()
                .terminal(terminal)
                .completer(completer)
                .parser(new DefaultParser())
                .variable(LineReader.LIST_MAX, 50)   // max tab completion candidates
                .expander(new NoopExpander())
                .build();
        reader.setAutosuggestion(SuggestionType.COMPLETER);

        RegistryCommandExecutor call = createCall(reader);
        // start the shell and process input until the user quits with Ctrl-D
        while (true) {
            try {
                String line = reader.readLine(PROMPT, null, (MaskingCallback) null, null).trim();
                if (line.isEmpty()) {
                    continue;
                }
                if (line.startsWith(INTERNAL_COMMAND_PREFIX)) {
                    executeInternalCommand(call, line);
                } else {
                    executeSqlQuery(sqlManager, line);
                    completer.refreshSchema();
                }
            } catch (UserInterruptException e) {
                // Ignore
            } catch (EndOfFileException e) {
                break;
            }
        }
    }

    private RegistryCommandExecutor createCall(LineReader reader) {
        SystemRegistryImpl registry = replExecutor.createRegistry();
        PicocliCommands picocliCommands = replExecutor.createPicocliCommands(SqlReplTopLevelCliCommand.class);
        registry.setCommandRegistries(picocliCommands);
        return new RegistryCommandExecutor(registry, picocliCommands, reader, null);
    }

    private void executeSqlQuery(SqlManager sqlManager, String line) {
        DefaultCallExecutionPipeline.builder(new SqlQueryCall(sqlManager))
                .inputProvider(() -> new ReplCallInput(line))
                .output(System.out)
                .errOutput(System.err)
                .decorator(new SqlQueryResultDecorator())
                .build()
                .runPipeline();
    }

    private void executeInternalCommand(RegistryCommandExecutor call, String line) {
        DefaultCallExecutionPipeline.builder(call)
                .inputProvider(() -> new ReplCallInput(line.substring(INTERNAL_COMMAND_PREFIX.length())))
                .output(System.out)
                .errOutput(System.err)
                .build()
                .runPipeline();
    }
}
