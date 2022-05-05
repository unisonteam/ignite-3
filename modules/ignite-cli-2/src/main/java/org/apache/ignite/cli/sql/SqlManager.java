/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.cli.sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.ignite.cli.sql.table.Table;

/**
 * Manager to work with any sql operation.
 */
public class SqlManager implements AutoCloseable {
    private final Connection connection;

    public SqlManager(String jdbcUrl) throws SQLException {
        connection = DriverManager.getConnection(jdbcUrl);
    }

    /**
     * Execute provided string as SQL request.
     *
     * @param sql incoming string representation of SQL command.
     * @return result of provided SQL command in terms of {@link Table}.
     * @throws SQLException in any case when SQL command can't be executed.
     */
    public Table<String> execute(String sql) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
            ResultSet resultSet = statement.getResultSet();
            return Table.fromResultSet(resultSet);
        }
    }

    @Override
    public void close() throws Exception {
        connection.close();
    }
}
