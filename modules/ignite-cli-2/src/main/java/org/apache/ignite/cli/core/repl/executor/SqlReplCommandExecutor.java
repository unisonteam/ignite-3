package org.apache.ignite.cli.core.repl.executor;

import org.apache.ignite.cli.call.configuration.ReplCallInput;
import org.apache.ignite.cli.core.call.Call;
import org.apache.ignite.cli.core.call.CallOutput;
import org.apache.ignite.cli.core.call.DefaultCallOutput;
import org.apache.ignite.cli.sql.table.Table;

public class SqlReplCommandExecutor implements Call<ReplCallInput, String> {

    private final SqlExecutor sqlExecutor;

    public SqlReplCommandExecutor(SqlExecutor sqlExecutor) {
        this.sqlExecutor = sqlExecutor;
    }

    @Override
    public CallOutput<String> execute(ReplCallInput input) {
        try {
            Table<String> result = sqlExecutor.execute(input.getLine());
            return DefaultCallOutput.success(String.valueOf(result));
        } catch (Exception e) {
            return DefaultCallOutput.failure(e);
        }
    }
}
