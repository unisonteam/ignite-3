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

package org.apache.ignite.catalog.sql;

import static org.apache.ignite.catalog.sql.CreateTableFromAnnotationsImpl.processColumns;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;
import org.apache.ignite.catalog.IndexColumn;
import org.apache.ignite.catalog.IndexType;
import org.apache.ignite.catalog.Options;
import org.apache.ignite.catalog.Query;
import org.apache.ignite.catalog.TableDefinition;
import org.apache.ignite.catalog.annotations.Table;
import org.apache.ignite.sql.IgniteSql;

class CreateTableFromBuilderImpl extends AbstractCatalogQuery {

    private final CreateTableImpl createTable;

    CreateTableFromBuilderImpl(IgniteSql sql, Options options) {
        super(sql, options);
        createTable = new CreateTableImpl(sql, options);
    }

    Query from(TableDefinition def) {
        createTable.name(def.getSchemaName(), def.getTableName());
        if (def.ifNotExists()) {
            createTable.ifNotExists();
        }
        if (!isEmpty(def.getColocationColumns())) {
            createTable.colocateBy(def.getColocationColumns());
        }
        if (def.getZone() != null) {
            createTable.zone(def.getZone());
        }
        if (!isEmpty(def.getAnnotatedClasses())) {
            for (Class<?> clazz : def.getAnnotatedClasses()) {
                var pkType = Optional.ofNullable(clazz.getAnnotation(Table.class)).map(Table::primaryKeyType).orElse(IndexType.DEFAULT);
                processColumns(createTable, pkType, clazz);
            }
        } else {
            if (!isEmpty(def.getColumns())) {
                for (var column : def.getColumns()) {
                    if (column.getType() != null) {
                        createTable.column(column.getName(), column.getType());
                    } else if (column.getDefinition() != null) {
                        createTable.column(column.getName(), column.getDefinition());
                    }
                }
            }
            if (!isEmpty(def.getPrimaryKeyColumns())) {
                var type = def.getPrimaryKeyType() == null ? IndexType.DEFAULT : def.getPrimaryKeyType();
                createTable.primaryKey(type, def.getPrimaryKeyColumns());
            }
        }
        // todo indexes
        return this;
    }

    private static boolean isEmpty(Collection<?> c) {
        return c == null || c.isEmpty();
    }

    @Override
    protected void accept(QueryContext ctx) {
        ctx.visit(createTable);
    }

    private static String toIndexName(IndexColumn... columns) {
        var list = new ArrayList<String>();
        list.add("ix");
        for (var col : columns) {
            list.add(col.getColumnName());
        }
        return String.join("_", list);
    }
}
