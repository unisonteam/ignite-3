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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.ignite.cli.sql.table.Table;

/**
 * Manager to work with any sql operation.
 */
public class SqlManager implements AutoCloseable {
    private final Connection connection;
    private Map<String, Map<String, Set<String>>> schema;

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
    public SqlQueryResult execute(String sql) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            if (statement.execute(sql)) {
                ResultSet resultSet = statement.getResultSet();
                return new SqlQueryResult(Table.fromResultSet(resultSet));
            }
            int updateCount = statement.getUpdateCount();
            return new SqlQueryResult(updateCount >= 0 ? "Updated " + updateCount + " rows." : "OK!");
        }
    }

    /**
     * Retrieves DB schema. Initially set of column names is null, it can be populated with the {@link SqlManager#getColumnNames(String)}
     *
     * @return map from schema name to the map from table name to nullable set of column names
     */
    public Map<String, Map<String, Set<String>>> getSchema() {
        if (schema == null) {
            schema = new HashMap<>();
            try (ResultSet rs = connection.getMetaData().getTables(null, null, null, null)) {
                while (rs.next()) {
                    String tableSchema = rs.getString("TABLE_SCHEM");
                    Map<String, Set<String>> tables = schema.computeIfAbsent(tableSchema, schemaName -> new HashMap<>());
                    tables.put(rs.getString("TABLE_NAME"), null);
                }
            } catch (SQLException e) {
                //todo report error
            }
        }
        return schema;
    }

    /**
     * Retrieves column names and stores it in the schema cache.
     * @param tableName name of the table.
     * @return set of column names.
     */
    public Set<String> getColumnNames(String tableName) {
        Entry<String, Map<String, Set<String>>> schema = findSchema(tableName);
        if (schema != null) {
            Map<String, Set<String>> tables = schema.getValue();
            return tables.computeIfAbsent(tableName, key -> getColumns(schema.getKey(), key));
        }
        return Collections.emptySet();
    }

    /**
     * Retrieves column names from the metadata.
     * @param schemaName name of the schema.
     * @param tableName name of the table.
     * @return set of column names.
     */
    private Set<String> getColumns(String schemaName, String tableName) {
        Map<String, Set<String>> tables = schema.get(schemaName);
        if (tables != null) {
            if (!tables.containsKey(tableName)) {
                try (ResultSet rs = connection.getMetaData().getColumns(null, schemaName, tableName, null)) {
                    while (rs.next()) {
                        Set<String> columns = tables.computeIfAbsent(tableName, key -> new HashSet<>());
                        columns.add(rs.getString("COLUMN_NAME"));
                    }
                } catch (SQLException e) {
                    //todo report error
                }
            }
            return tables.getOrDefault(tableName, Collections.emptySet());
        }
        return Collections.emptySet();
    }

    /**
     * Finds table schema which contains a table with the given name.
     * @param tableName name of the table to find.
     * @return map entry for the schema or null if table is not found.
     */
    private Entry<String, Map<String, Set<String>>> findSchema(String tableName) {
        Map<String, Map<String, Set<String>>> schema = getSchema();
        for (Entry<String, Map<String, Set<String>>> entry : schema.entrySet()) {
            if (entry.getValue().containsKey(tableName)) {
                return entry;
            }
        }
        return null;
    }

    /**
     * Resets stored DB schema so the next {@link SqlManager#getSchema()} call will retrieve it to accommodate possible changes.
     */
    public void invalidateSchema() {
        schema = null;
    }

    @Override
    public void close() throws Exception {
        connection.close();
    }
}
