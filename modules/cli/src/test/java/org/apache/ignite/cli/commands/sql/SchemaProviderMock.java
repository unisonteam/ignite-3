package org.apache.ignite.cli.commands.sql;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.cli.sql.SchemaProvider;

class SchemaProviderMock implements SchemaProvider {
    @Override
    public Map<String, Map<String, Set<String>>> getSchema() {
        return Map.of("PUBLIC", Map.of("PERSON", Set.of("ID", "NAME", "SALARY")));
    }

    @Override
    public Set<String> getColumnNames(String tableName) {
        return Collections.emptySet();
    }

    @Override
    public void invalidateSchema() {
    }
}
