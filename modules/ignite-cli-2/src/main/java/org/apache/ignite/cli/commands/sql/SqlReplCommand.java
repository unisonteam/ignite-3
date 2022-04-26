package org.apache.ignite.cli.commands.sql;

import jakarta.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.concurrent.Callable;
import org.apache.ignite.cli.sql.SqlManager;
import org.apache.ignite.cli.sql.table.Table;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Command for sql execution.
 */
@Command(name = "sql")
public class SqlReplCommand implements Callable<Table<String>> {
    @Option(names = {"--jdbc-url"}, required = true)
    private String jdbc;
    @Option(names = {"-execute", "--execute"})
    private String command;
    @Option(names = {"--script-file"})
    private File file;

    @Inject
    private SqlReplExecutor sqlReplExecutor;

    /**
     * Run method.
     *
     * @return Table with result of command.
     * @throws Exception in any case of sql execution.
     */
    @Override
    public Table<String> call() throws Exception {
        try (SqlManager sqlManager = new SqlManager(jdbc)) {
            if (command == null && file == null) {
                sqlReplExecutor.executeRepl(sqlManager);
                return null;
            } else {
                String executeCommand = file != null ? extract(file) : command;
                return sqlManager.execute(executeCommand);
            }
        }
    }

    private static String extract(File file) throws IOException {
        return String.join("\n", Files.readAllLines(file.toPath(), StandardCharsets.UTF_8));
    }
}
