package org.apache.ignite.cli.core.repl.executor;

import org.apache.ignite.cli.core.call.Call;
import org.apache.ignite.cli.core.call.CallOutput;
import org.apache.ignite.cli.core.call.DefaultCallOutput;
import org.apache.ignite.cli.core.call.StringCallInput;
import org.apache.ignite.cli.sql.SqlManager;
import org.apache.ignite.cli.sql.SqlQueryResult;

/**
 * Call implementation for SQL command execution.
 */
public class SqlQueryCall implements Call<StringCallInput, SqlQueryResult> {

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
    public CallOutput<SqlQueryResult> execute(StringCallInput input) {
        return DefaultCallOutput.wrapCall(() -> sqlManager.execute(input.getString()));
    }
}
