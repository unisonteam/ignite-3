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

package org.apache.ignite.ddl.sql;

import static org.apache.ignite.table.mapper.Mapper.nativelySupported;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import org.apache.ignite.ddl.DefaultZone;
import org.apache.ignite.ddl.Options;
import org.apache.ignite.ddl.Query;
import org.apache.ignite.ddl.annotations.Column;
import org.apache.ignite.ddl.annotations.Id;
import org.apache.ignite.ddl.annotations.Index;
import org.apache.ignite.ddl.annotations.Table;
import org.apache.ignite.ddl.annotations.Zone;
import org.apache.ignite.sql.IgniteSql;

class CreateTableFromClassImpl extends AbstractDdlQuery {

    private CreateZoneImpl createZone;
    private final CreateTableImpl createTable;

    CreateTableFromClassImpl(IgniteSql sql, Options options) {
        super(sql, options);
        createTable = new CreateTableImpl(sql, options).ifNotExists();
    }

    Query keyValueView(Class<?> key, Class<?> value) {
        processAnnotations(key);
        processAnnotations(value);
        return this;
    }

    Query recordView(Class<?> recCls) {
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
        var table = clazz.getAnnotation(Table.class);
        if (table != null) {
            processZone(table);

            createTable.name(table.name().isEmpty() ? clazz.getSimpleName() : table.name());
            processTable(table);
        }
        processColumns(clazz);
    }

    private void processZone(Table table) {
        var zoneRef = table.zone();
        if (zoneRef == DefaultZone.class) {
            return;
        }
        var zone = zoneRef.getAnnotation(Zone.class);
        if (zone != null) {
            createZone = new CreateZoneImpl(sql, options()).ifNotExists();

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
        var indexes = table.indexes();
        for (Index ix : indexes) {
            createTable.index(ix.name(), ix.type(), ix.columnList());
        }

        var colocateBy = table.colocateBy().columnList();
        if (!colocateBy.isEmpty()) {
            createTable.colocateBy(colocateBy);
        }
    }

    private void processColumns(Class<?> clazz) {
        var pkColumns = new ArrayList<String>();

        if (nativelySupported(clazz)) {
            // e.g. primitive boxed key in keyValueView
            pkColumns.add("id");
            createTable.column("id", ColumnType.of(clazz));
        } else {
            processColumnsInPojo(clazz, pkColumns);
        }

        if (!pkColumns.isEmpty()) {
            createTable.primaryKey(pkColumns.toArray(new String[0]));
        }
    }

    private void processColumnsInPojo(Class<?> clazz, ArrayList<String> pkColumns) {
        for (Field f : clazz.getDeclaredFields()) {
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
                    ColumnType<?> type = ColumnType.of(f.getType());
                    type.length(column.length() > 0 ? column.length() : null);
                    type.precision(column.precision() > 0 ? column.precision() : null);
                    type.scale(column.scale() > 0 ? column.scale() : null);
                    type.nullable(column.nullable());
                    createTable.column(columnName, type);
                }
            }

            if (f.getAnnotation(Id.class) != null) {
                pkColumns.add(columnName);
            }
        }
    }
}
