package org.apache.ignite.cli.core.repl.executor;

import java.util.Objects;
import org.apache.ignite.cli.call.configuration.ReplCallInput;
import org.apache.ignite.cli.core.call.Call;
import org.apache.ignite.cli.core.call.CallOutput;
import org.apache.ignite.cli.core.call.DefaultCallOutput;
import org.apache.ignite.cli.sql.table.Table;
import org.jline.terminal.Terminal;
import org.jline.utils.InfoCmp.Capability;

public class SqlReplCommandExecutor implements Call<ReplCallInput, String> {

    private static final String COMMAND_PREFIX = "!";
    private final Terminal terminal;
    private final SqlExecutor sqlExecutor;

    public SqlReplCommandExecutor(Terminal terminal, SqlExecutor sqlExecutor) {
        this.terminal = terminal;
        this.sqlExecutor = sqlExecutor;
    }

    @Override
    public CallOutput<String> execute(ReplCallInput input) {
        String line = input.getLine().trim();
        if (line.startsWith(COMMAND_PREFIX)) {
            if (Objects.equals(line, "clear")) {
                terminal.puts(Capability.clear_screen);
                return DefaultCallOutput.success(null);
            }
        }
        try {
            Table<String> result = sqlExecutor.execute(line);
            return DefaultCallOutput.success(String.valueOf(result));
        } catch (Exception e) {
            return DefaultCallOutput.failure(e);
        }
    }
}
