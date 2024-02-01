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

import static org.apache.ignite.catalog.ColumnSorted.column;
import static org.apache.ignite.table.mapper.Mapper.nativelySupported;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.catalog.ColumnSorted;
import org.apache.ignite.catalog.ColumnType;
import org.apache.ignite.catalog.DefaultZone;
import org.apache.ignite.catalog.IndexType;
import org.apache.ignite.catalog.Options;
import org.apache.ignite.catalog.annotations.Col;
import org.apache.ignite.catalog.annotations.Column;
import org.apache.ignite.catalog.annotations.Id;
import org.apache.ignite.catalog.annotations.Index;
import org.apache.ignite.catalog.annotations.Table;
import org.apache.ignite.catalog.annotations.Zone;
import org.apache.ignite.sql.IgniteSql;

class CreateTableFromAnnotationsImpl extends AbstractCatalogQuery {

    private boolean ifNotExists;
    private CreateZoneImpl createZone;
    private CreateTableImpl createTable;
    private IndexType pkType;

    CreateTableFromAnnotationsImpl(IgniteSql sql, Options options) {
        super(sql, options);
    }

    CreateTableFromAnnotationsImpl ifNotExists() {
        this.ifNotExists = true;
        return this;
    }

    CreateTableFromAnnotationsImpl keyValueView(Class<?> key, Class<?> value) {
        processAnnotations(key);
        processAnnotations(value);
        return this;
    }

    CreateTableFromAnnotationsImpl recordView(Class<?> recCls) {
        processAnnotations(recCls);
        return this;
    }

    @Override
    protected void accept(QueryContext ctx) {
        if (createZone != null) {
            ctx.visit(createZone).formatSeparator().sql("\n");
        }
        ctx.visit(createTable);
    }

    private void processAnnotations(Class<?> clazz) {
        if (createTable == null) {
            createTable = new CreateTableImpl(sql, options());
            if (ifNotExists) {
                createTable.ifNotExists();
            }
        }
        var table = clazz.getAnnotation(Table.class);
        if (table != null) {
            processZone(table);

            createTable.name(table.name().isEmpty() ? clazz.getSimpleName() : table.name());
            processTable(table);
        }
        processColumns(createTable, pkType, clazz);
    }

    private void processZone(Table table) {
        var zoneRef = table.zone();
        if (zoneRef == DefaultZone.class) {
            return;
        }
        var zone = zoneRef.getAnnotation(Zone.class);
        if (zone != null) {
            createZone = new CreateZoneImpl(sql, options());
            if (ifNotExists) {
                createZone.ifNotExists();
            }

            var zoneName = zone.name().isEmpty() ? zoneRef.getSimpleName() : zone.name();
            createTable.zone(zoneName);
            createZone.name(zoneName);
            createZone.engine(zone.engine());
            if (zone.partitions() > 0) {
                createZone.partitions(zone.partitions());
            }
            if (zone.replicas() > 0) {
                createZone.replicas(zone.replicas());
            }
            // todo complete building filter, region, adjust etc.
        }
    }

    private void processTable(Table table) {
        for (var ix : table.indexes()) {
            var indexColumns = map(ix.columns(), col -> column(col.name(), col.sort()));
            var name = toIndexName(ix);
            createTable.index(name, ix.type(), indexColumns);
        }

        var colocateBy = table.colocateBy().columns();
        if (colocateBy != null && colocateBy.length > 0) {
            createTable.colocateBy(map(colocateBy, Col::name));
        }

        pkType = table.primaryKeyType();
    }

    private static String toIndexName(Index ix) {
        if (!ix.name().isEmpty()) {
            return ix.name();
        }
        var list = new ArrayList<String>();
        list.add("ix");
        for (var col : ix.columns()) {
            list.add(col.name());
        }
        return String.join("_", list);
    }

    static void processColumns(CreateTableImpl createTable, IndexType pkType, Class<?> clazz) {
        var idColumns = new ArrayList<ColumnSorted>();

        if (nativelySupported(clazz)) {
            // e.g. primitive boxed key in keyValueView
            idColumns.add(column("id"));
            createTable.column("id", ColumnType.of(clazz));
        } else {
            processColumnsInPojo(createTable, clazz, idColumns);
        }

        if (!idColumns.isEmpty()) {
            createTable.primaryKey(pkType, idColumns);
        }
    }

    private static void processColumnsInPojo(CreateTableImpl createTable, Class<?> clazz, List<ColumnSorted> idColumns) {
        for (var f : clazz.getDeclaredFields()) {
            if (Modifier.isStatic(f.getModifiers()) || Modifier.isTransient(f.getModifiers())) {
                continue;
            }

            String columnName;
            var column = f.getAnnotation(Column.class);

            if (column == null) {
                columnName = f.getName();
                createTable.column(columnName, ColumnType.of(f.getType()));

            } else {
                columnName = column.name().isEmpty() ? f.getName() : column.name();

                if (!column.columnDefinition().isEmpty()) {
                    createTable.column(columnName, column.columnDefinition());
                } else {
                    var type = ColumnType.of(f.getType(), column.length(), column.precision(), column.scale(), column.nullable());
                    createTable.column(columnName, type);
                }
            }

            var id = f.getAnnotation(Id.class);
            if (id != null) {
                idColumns.add(column(columnName).sort(id.sort()));
            }
        }
    }
}
