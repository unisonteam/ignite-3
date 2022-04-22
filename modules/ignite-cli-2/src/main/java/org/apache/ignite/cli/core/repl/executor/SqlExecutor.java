package org.apache.ignite.cli.core.repl.executor;

import org.apache.ignite.cli.sql.table.Table;

public interface SqlExecutor {
    Table<String> execute(String sql) throws Exception;
}
