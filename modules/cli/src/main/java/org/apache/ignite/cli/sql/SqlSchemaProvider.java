package org.apache.ignite.cli.sql;

import java.time.Duration;
import java.time.Instant;

/**
 * SQL schema provider.
 */
public class SqlSchemaProvider implements SchemaProvider {
    private static final int SCHEMA_UPDATE_TIMEOUT = 10;

    private final SqlSchemaLoader sqlSchemaLoader;
    private final int schemaUpdateTimeout;
    private SqlSchema schema;
    private Instant lastUpdate;

    public SqlSchemaProvider(MetadataSupplier metadataSupplier) {
        this(metadataSupplier, SCHEMA_UPDATE_TIMEOUT);
    }

    SqlSchemaProvider(MetadataSupplier metadataSupplier, int schemaUpdateTimeout) {
        sqlSchemaLoader = new SqlSchemaLoader(metadataSupplier);
        this.schemaUpdateTimeout = schemaUpdateTimeout;
    }

    @Override
    public SqlSchema getSchema() {
        Instant now = Instant.now();
        if (schema == null || Duration.between(lastUpdate, now).toSeconds() >= schemaUpdateTimeout) {
            lastUpdate = Instant.now();
            schema = sqlSchemaLoader.loadSchema();
        }
        return schema;
    }

}