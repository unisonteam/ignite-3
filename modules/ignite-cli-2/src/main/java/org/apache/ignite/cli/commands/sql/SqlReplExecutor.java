package org.apache.ignite.cli.commands.sql;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.ignite.cli.call.configuration.ReplCallInput;
import org.apache.ignite.cli.commands.decorators.TableDecorator;
import org.apache.ignite.cli.core.call.DefaultCallExecutionPipeline;
import org.apache.ignite.cli.core.repl.executor.RegistryCommandExecutor;
import org.apache.ignite.cli.core.repl.executor.ReplExecutor;
import org.apache.ignite.cli.core.repl.executor.SqlQueryCall;
import org.apache.ignite.cli.sql.SqlManager;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.MaskingCallback;
import org.jline.reader.UserInterruptException;
import org.jline.reader.impl.DefaultParser;
import org.jline.terminal.Terminal;

/**
 * SQL REPL command executor.
 */
@Singleton
public class SqlReplExecutor {
    private static final String PROMPT = "sql-cli> ";
    private static final String COMMAND_PREFIX = "!";

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
        LineReader reader = LineReaderBuilder.builder()
                .terminal(terminal)
                //.completer()
                .parser(new DefaultParser())
                .variable(LineReader.LIST_MAX, 50)   // max tab completion candidates
                .build();
        SqlQueryCall executor = new SqlQueryCall(sqlManager);
        RegistryCommandExecutor call =
                new RegistryCommandExecutor(replExecutor.createRegistry(),
                        replExecutor.createPicocliCommands(SqlReplTopLevelCliCommand.class),
                        reader, false);
        // start the shell and process input until the user quits with Ctrl-D
        while (true) {
            try {
                String line = reader.readLine(PROMPT, null, (MaskingCallback) null, null).trim();
                if (line.startsWith(COMMAND_PREFIX)) {
                    DefaultCallExecutionPipeline.builder(call)
                            .inputProvider(() -> new ReplCallInput(line.substring(COMMAND_PREFIX.length())))
                            .output(System.out)
                            .errOutput(System.err)
                            .build()
                            .runPipeline();
                } else {
                    DefaultCallExecutionPipeline.builder(executor)
                            .inputProvider(() -> new ReplCallInput(line))
                            .output(System.out)
                            .errOutput(System.err)
                            .decorator(new TableDecorator())
                            .build()
                            .runPipeline();
                }
            } catch (UserInterruptException e) {
                // Ignore
            } catch (EndOfFileException e) {
                break;
            }
        }
    }
}
