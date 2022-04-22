package org.apache.ignite.cli.core.repl.executor;

import org.apache.ignite.cli.sql.table.Table;

/**
 * Executor of SQL command.
 */
public interface SqlExecutor {
    /**
     * Execute provided string as SQL request.
     *
     * @param sql incoming string representation of SQL command.
     * @return result of provided SQL command in term of {@link Table}.
     * @throws Exception in any case when SQL command can't be executed.
     */
    Table<String> execute(String sql) throws Exception;
}
