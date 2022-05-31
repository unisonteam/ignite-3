package org.apache.ignite.cli.sql;

/**
 * Database schema provider.
 */
public interface SchemaProvider {
    /**
     * Retrieves DB schema.
     *
     * @return map from schema name to the map from table name to nullable set of column names.
     */
    SqlSchema getSchema();
}
