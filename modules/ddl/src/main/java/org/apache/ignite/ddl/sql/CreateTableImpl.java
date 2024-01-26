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

import static java.util.Arrays.asList;
import static org.apache.ignite.ddl.sql.IndexColumnImpl.parseIndexColumnList;
import static org.apache.ignite.ddl.sql.QueryPartCollection.wrap;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.ignite.ddl.IndexType;
import org.apache.ignite.ddl.Options;
import org.apache.ignite.sql.IgniteSql;

class CreateTableImpl extends AbstractDdlQuery {

    private Name tableName;
    private boolean ifNotExists;
    private final List<Column> columns = new ArrayList<>();
    private final List<Constraint> constraints = new ArrayList<>();
    private final List<WithOption> withOptions = new ArrayList<>();
    private Colocate colocate;
    private final List<CreateIndexImpl> indexes = new ArrayList<>();

    /**
     * Constructor for internal usage.
     *
     * @see CreateTableFromClassImpl
     */
    CreateTableImpl(IgniteSql sql, Options options) {
        super(sql, options);
    }

    CreateTableImpl name(String... names) {
        Objects.requireNonNull(names, "table name is null");
        this.tableName = new Name(names);
        return this;
    }

    CreateTableImpl ifNotExists() {
        this.ifNotExists = true;
        return this;
    }

    CreateTableImpl column(String name, String definition) {
        Objects.requireNonNull(name, "column name is null");
        Objects.requireNonNull(definition, "column type is null");
        columns.add(new Column(name, definition));
        return this;
    }

    CreateTableImpl column(String name, ColumnType<?> type) {
        Objects.requireNonNull(name, "column name is null");
        Objects.requireNonNull(type, "column type is null");
        columns.add(new Column(name, ColumnTypeImpl.wrap(type)));
        return this;
    }

    CreateTableImpl primaryKey(String columnList) {
        return primaryKey(IndexType.DEFAULT, columnList);
    }

    CreateTableImpl primaryKey(IndexType type, String columnList) {
        return primaryKey(type, parseIndexColumnList(columnList));
    }

    CreateTableImpl primaryKey(IndexType type, List<IndexColumn> columns) {
        Objects.requireNonNull(columns, "pk columns is null");
        constraints.add(new Constraint().primaryKey(type, columns));
        return this;
    }

    CreateTableImpl colocateBy(String columnList) {
        return colocateBy(columnList.split("\\s*,\\s*"));
    }

    CreateTableImpl colocateBy(String... columns) {
        return colocateBy(asList(columns));
    }

    CreateTableImpl colocateBy(List<String> columns) {
        Objects.requireNonNull(columns, "colocate columns is null");
        colocate = new Colocate(columns);
        return this;
    }

    CreateTableImpl zone(String zone) {
        Objects.requireNonNull(zone, "zone is null");
        withOptions.add(WithOption.primaryZone(zone));
        return this;
    }

    CreateTableImpl index(String name, String columnList) {
        return index(name, null, columnList);
    }

    CreateTableImpl index(String name, IndexType type, String columnList) {
        return index(name, type, parseIndexColumnList(columnList));
    }

    CreateTableImpl index(String name, IndexType type, IndexColumn... columns) {
        return index(name, type, asList(columns));
    }

    CreateTableImpl index(String name, IndexType type, List<IndexColumn> columns) {
        Objects.requireNonNull("index name is null");
        Objects.requireNonNull(columns);
        indexes.add(new CreateIndexImpl(sql, options()).ifNotExists().name(name).using(type).on(tableName, columns));
        return this;
    }

    @Override
    protected void accept(QueryContext ctx) {
        ctx.sql("CREATE TABLE ");
        if (ifNotExists) {
            ctx.sql("IF NOT EXISTS ");
        }
        ctx.visit(tableName);
        ctx.sqlIndentStart(" (");
        if (!columns.isEmpty()) {
            ctx.visit(wrap(columns).separator(", ").formatSeparator());
        }

        if (!constraints.isEmpty()) {
            ctx.sql(", ").formatSeparator();
            // todo merge primary keys
            ctx.visit(wrap(constraints).separator(", ").formatSeparator());
        }

        ctx.sqlIndentEnd(")");

        if (colocate != null) {
            ctx.sql(" ").formatSeparator().visit(colocate);
        }

        if (!withOptions.isEmpty()) {
            ctx.sql(" ").formatSeparator().sql("WITH ");
            ctx.visit(wrap(withOptions).separator(", ").formatSeparator());
        }

        ctx.sql(";");

        if (!indexes.isEmpty()) {
            for (var index : indexes) {
                ctx.sql("\n").formatSeparator();
                ctx.visit(index);
            }
        }
    }
}
