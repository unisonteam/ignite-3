package org.apache.ignite.cli.sql;

import java.util.Map;
import java.util.Set;

/**
 * Database schema provider.
 */
public interface SchemaProvider {
    /**
     * Retrieves DB schema. Initially set of column names is null, it can be populated with the
     * {@link SqlSchemaProvider#getColumnNames(String)}
     *
     * @return map from schema name to the map from table name to nullable set of column names
     */
    Map<String, Map<String, Set<String>>> getSchema();

    /**
     * Retrieves column names and stores it in the schema cache.
     *
     * @param tableName name of the table.
     * @return set of column names.
     */
    Set<String> getColumnNames(String tableName);

    /**
     * Resets stored DB schema so the next {@link SqlSchemaProvider#getSchema()} call will retrieve it to accommodate possible changes.
     */
    void invalidateSchema();
}
