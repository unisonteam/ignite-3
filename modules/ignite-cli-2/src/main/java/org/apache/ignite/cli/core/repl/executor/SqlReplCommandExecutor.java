package org.apache.ignite.cli.core.repl.executor;

import org.apache.ignite.cli.call.configuration.ReplCallInput;
import org.apache.ignite.cli.core.call.Call;
import org.apache.ignite.cli.core.call.CallOutput;
import org.apache.ignite.cli.core.call.DefaultCallOutput;
import org.apache.ignite.cli.sql.table.Table;

/**
 * Call implementation for SQL command execution.
 */
public class SqlReplCommandExecutor implements Call<ReplCallInput, Table<String>> {

    private final SqlExecutor sqlExecutor;

    /**
     * Constructor.
     *
     * @param sqlExecutor sql executor.
     */
    public SqlReplCommandExecutor(SqlExecutor sqlExecutor) {
        this.sqlExecutor = sqlExecutor;
    }

    /**
     * Cleanup system registry.
     */
    @Override
    public CallOutput<Table<String>> execute(ReplCallInput input) {
        try {
            Table<String> result = sqlExecutor.execute(input.getLine());
            return DefaultCallOutput.success(result);
        } catch (Exception e) {
            return DefaultCallOutput.failure(e);
        }
    }
}
