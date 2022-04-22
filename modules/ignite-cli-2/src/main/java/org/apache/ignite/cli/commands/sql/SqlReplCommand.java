package org.apache.ignite.cli.commands.sql;

import jakarta.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Objects;
import java.util.concurrent.Callable;
import org.apache.ignite.cli.call.configuration.ReplCallInput;
import org.apache.ignite.cli.core.call.DefaultCallExecutionPipeline;
import org.apache.ignite.cli.core.repl.executor.SqlExecutor;
import org.apache.ignite.cli.core.repl.executor.SqlReplCommandExecutor;
import org.apache.ignite.cli.sql.SqlManager;
import org.apache.ignite.cli.sql.table.Table;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.MaskingCallback;
import org.jline.reader.UserInterruptException;
import org.jline.reader.impl.DefaultParser;
import org.jline.terminal.Terminal;
import org.jline.utils.InfoCmp.Capability;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "sql")
public class SqlReplCommand implements Callable<Table<String>> {
    private static final String PROMPT = "sql-cli> ";
    private static final String COMMAND_PREFIX = "!";

    @Inject
    private Terminal terminal;

    @Option(names = {"--jdbc-url"}, required = true)
    private String jdbc;
    @Option(names = {"-execute", "--execute"})
    private String command;
    @Option(names = {"--script-file"})
    private File file;

    @Override
    public Table<String> call() throws Exception {
        try (SqlManager sqlManager = new SqlManager(jdbc)) {
            if (command == null && file == null) {
                return executeRepl(sqlManager);
            } else {
                String executeCommand = file != null ? extract(file) : command;
                return sqlManager.execute(executeCommand);
            }
        }
    }

    private Table<String> executeRepl(SqlExecutor sqlExecutor) {
        LineReader reader = LineReaderBuilder.builder()
                .terminal(terminal)
                //.completer()
                .parser(new DefaultParser())
                .variable(LineReader.LIST_MAX, 50)   // max tab completion candidates
                .build();
        SqlReplCommandExecutor executor = new SqlReplCommandExecutor(sqlExecutor);
        // start the shell and process input until the user quits with Ctrl-D
        while (true) {
            try {
                String line = reader.readLine(PROMPT, null, (MaskingCallback) null, null).trim();
                if (line.startsWith(COMMAND_PREFIX)) {
                    executeCommand(line.substring(COMMAND_PREFIX.length()));
                } else {
                    DefaultCallExecutionPipeline.builder(executor)
                            .inputProvider(() -> new ReplCallInput(line))
                            .output(new PrintWriter(System.out, true))
                            .errOutput(new PrintWriter(System.err, true))
                            .build()
                            .runPipeline();
                }
            } catch (UserInterruptException e) {
                // Ignore
            } catch (EndOfFileException e) {
                break;
            }
        }
        return null;
    }

    private void executeCommand(String line) {
        if (Objects.equals(line, "clear")) {
            terminal.puts(Capability.clear_screen);
        }
    }

    private static String extract(File file) throws IOException {
        return String.join("\n", Files.readAllLines(file.toPath(), StandardCharsets.UTF_8));
    }
}
