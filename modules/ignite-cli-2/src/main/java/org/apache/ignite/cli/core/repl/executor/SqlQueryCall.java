package org.apache.ignite.cli.core.repl.executor;

import java.sql.SQLException;
import org.apache.ignite.cli.call.configuration.ReplCallInput;
import org.apache.ignite.cli.core.call.Call;
import org.apache.ignite.cli.core.call.CallOutput;
import org.apache.ignite.cli.core.call.DefaultCallOutput;
import org.apache.ignite.cli.sql.SqlManager;
import org.apache.ignite.cli.sql.SqlQueryResult;

/**
 * Call implementation for SQL command execution.
 */
public class SqlQueryCall implements Call<ReplCallInput, SqlQueryResult> {

    private final SqlManager sqlManager;

    /**
     * Constructor.
     *
     * @param sqlManager SQL manager.
     */
    public SqlQueryCall(SqlManager sqlManager) {
        this.sqlManager = sqlManager;
    }

    /** {@inheritDoc} */
    @Override
    public CallOutput<SqlQueryResult> execute(ReplCallInput input) {
        try {
            SqlQueryResult result = sqlManager.execute(input.getLine());
            return DefaultCallOutput.success(result);
        } catch (SQLException e) {
            return DefaultCallOutput.failure(e);
        }
    }
}
