/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.catalog;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class TableDefinition {

    private final String tableName;
    private final String schemaName;
    private final boolean ifNotExists;
    private final List<ColumnDefinition> columns;
    private final IndexType pkType;
    private final List<ColumnSorted> pkColumns;

    private final List<String> colocationColumns;
    private final String zoneName;
    private final List<Class<?>> annotatedClasses;
    private final List<IndexDefinition> indexes;

    private TableDefinition(
            String tableName,
            String schemaName,
            boolean ifNotExists,
            List<ColumnDefinition> columns,
            IndexType pkType,
            List<ColumnSorted> pkColumns,
            List<String> colocationColumns,
            String zoneName,
            List<Class<?>> annotatedClasses,
            List<IndexDefinition> indexes) {
        this.tableName = tableName;
        this.schemaName = schemaName;
        this.ifNotExists = ifNotExists;
        this.columns = columns;
        this.pkType = pkType;
        this.pkColumns = pkColumns;
        this.colocationColumns = colocationColumns;
        this.zoneName = zoneName;
        this.annotatedClasses = annotatedClasses;
        this.indexes = indexes;
    }

    public static Builder builder(String tableName) {
        return new Builder().tableName(tableName);
    }

    public String getTableName() {
        return tableName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public boolean ifNotExists() {
        return ifNotExists;
    }

    public List<ColumnDefinition> getColumns() {
        return columns;
    }

    public IndexType getPrimaryKeyType() {
        return pkType;
    }

    public List<ColumnSorted> getPrimaryKeyColumns() {
        return pkColumns;
    }

    public String getZone() {
        return zoneName;
    }

    public List<String> getColocationColumns() {
        return colocationColumns;
    }

    public List<Class<?>> getAnnotatedClasses() {
        return annotatedClasses;
    }

    public List<IndexDefinition> getIndexes() {
        return indexes;
    }

    public Builder toBuilder() {
        return new Builder(this);
    }

    public static class Builder {
        private String tableName;
        private String schemaName;
        private boolean ifNotExists;
        private List<ColumnDefinition> columns;

        private IndexType pkType;
        private List<ColumnSorted> pkColumns;
        private List<String> colocationColumns;

        private String zoneName;

        private List<Class<?>> annotatedClasses;
        private List<IndexDefinition> indexes = new ArrayList<>();

        private Builder() {}

        private Builder(TableDefinition definition) {
            tableName = definition.tableName;
            schemaName = definition.schemaName;
            ifNotExists = definition.ifNotExists;
            columns = definition.columns;
            pkType = definition.pkType;
            pkColumns = definition.pkColumns;
            colocationColumns = definition.colocationColumns;
            zoneName = definition.zoneName;
            annotatedClasses = definition.annotatedClasses;
        }

        Builder tableName(String name) {
            this.tableName = name;
            return this;
        }

        public Builder schema(String schema) {
            this.schemaName = schema;
            return this;
        }

        public Builder ifNotExists() {
            this.ifNotExists = true;
            return this;
        }

        public Builder columns(ColumnDefinition... columns) {
            return columns(Arrays.asList(columns));
        }

        public Builder columns(List<ColumnDefinition> columns) {
            this.columns = columns;
            return this;
        }

        public Builder colocateBy(String... columns) {
            this.colocationColumns = Arrays.asList(columns);
            return this;
        }

        public Builder zone(String zoneName) {
            this.zoneName = zoneName;
            return this;
        }

        public Builder keyValueView(Class<?> key, Class<?> value) {
            annotatedClasses = List.of(key, value);
            return this;
        }

        public Builder recordView(Class<?> recClass) {
            annotatedClasses = List.of(recClass);
            return this;
        }

        public Builder primaryKey(String... columns) {
            var pkColumns = Arrays.stream(columns).map(ColumnSorted::column).collect(Collectors.toList());
            return primaryKey(IndexType.DEFAULT, pkColumns);
        }

        public Builder primaryKey(IndexType type, ColumnSorted... columns) {
            return primaryKey(type, Arrays.asList(columns));
        }

        public Builder primaryKey(IndexType type, List<ColumnSorted> columns) {
            pkType = type;
            pkColumns = columns;
            return this;
        }

        public Builder index(String... columns) {
            var columnsArr = Arrays.stream(columns).map(ColumnSorted::column).collect(Collectors.toList());
            return index(null, IndexType.DEFAULT, columnsArr);
        }

        public Builder index(String indexName, IndexType type, ColumnSorted... columns) {
            return index(indexName, type, Arrays.asList(columns));
        }

        public Builder index(String name, IndexType type, List<ColumnSorted> columns) {
            indexes.add(new IndexDefinition(name, type, columns));
            return this;
        }

        public TableDefinition build() {
            return new TableDefinition(
                    tableName,
                    schemaName,
                    ifNotExists,
                    columns,
                    pkType,
                    pkColumns,
                    colocationColumns,
                    zoneName,
                    annotatedClasses,
                    indexes
            );
        }
    }
}
