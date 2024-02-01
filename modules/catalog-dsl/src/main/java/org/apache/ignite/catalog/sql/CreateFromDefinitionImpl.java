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

import static org.apache.ignite.catalog.sql.CreateFromAnnotationsImpl.processColumns;

import java.util.ArrayList;
import java.util.Optional;
import org.apache.ignite.catalog.IndexDefinition;
import org.apache.ignite.catalog.IndexType;
import org.apache.ignite.catalog.Options;
import org.apache.ignite.catalog.Query;
import org.apache.ignite.catalog.TableDefinition;
import org.apache.ignite.catalog.ZoneDefinition;
import org.apache.ignite.catalog.annotations.Table;
import org.apache.ignite.sql.IgniteSql;

class CreateFromDefinitionImpl extends AbstractCatalogQuery {

    private CreateZoneImpl createZone;
    private CreateTableImpl createTable;

    CreateFromDefinitionImpl(IgniteSql sql, Options options) {
        super(sql, options);
    }

    Query from(ZoneDefinition def) {
        createZone = new CreateZoneImpl(sql, options());
        createZone.name(def.getZoneName());
        if (def.ifNotExists()) {
            createZone.ifNotExists();
        }
        if (isGreaterThanZero(def.getPartitions())) {
            createZone.partitions(def.getPartitions());
        }
        if (isGreaterThanZero(def.getReplicas())) {
            createZone.replicas(def.getReplicas());
        }
        createZone.engine(def.getEngine());
        // todo complete building filter, region, adjust etc.
        return this;
    }

    Query from(TableDefinition def) {
        createTable = new CreateTableImpl(sql, options());
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
        if (!isEmpty(def.getIndexes())) {
            for (var ix : def.getIndexes()) {
                createTable.index(toIndexName(ix), ix.getIndexType(), ix.getColumns());
            }
        }
        return this;
    }

    @Override
    protected void accept(QueryContext ctx) {
        var separator = "";
        for (var create : new QueryPart[]{createZone, createTable}) {
            if (create == null) {
                continue;
            }
            ctx.visit(create).formatSeparator().sql(separator);
            separator = "\n";
        }
    }

    private static String toIndexName(IndexDefinition ix) {
        if (ix.getIndexName() != null && !ix.getIndexName().isEmpty()) {
            return ix.getIndexName();
        }
        var list = new ArrayList<String>();
        list.add("ix");
        for (var col : ix.getColumns()) {
            list.add(col.getColumnName());
        }
        return String.join("_", list);
    }
}
