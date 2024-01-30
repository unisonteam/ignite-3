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

import static org.apache.ignite.ddl.sql.CreateTableFromClassImpl.processColumns;

import java.util.ArrayList;
import org.apache.ignite.ddl.CreateTable;
import org.apache.ignite.ddl.IndexColumn;
import org.apache.ignite.ddl.IndexType;
import org.apache.ignite.ddl.Options;
import org.apache.ignite.sql.IgniteSql;

class CreateTableFromClassExtendedImpl extends AbstractDdlQuery implements CreateTable {

    private final CreateTableImpl createTable;
    private IndexType pkType = IndexType.DEFAULT;

    CreateTableFromClassExtendedImpl(IgniteSql sql, Options options) {
        super(sql, options);
        createTable = new CreateTableImpl(sql, options);
    }

    CreateTableFromClassExtendedImpl name(String name) {
        createTable.name(name);
        return this;
    }

    @Override
    public CreateTable ifNotExists() {
        createTable.ifNotExists();
        return this;
    }

    @Override
    public CreateTable primaryKeyType(IndexType pkType) {
        this.pkType = pkType;
        return this;
    }

    @Override
    public CreateTable index(IndexType type, IndexColumn... columns) {
        createTable.index(toIndexName(columns), type, columns);
        return this;
    }

    @Override
    public CreateTable colocateBy(String... columns) {
        createTable.colocateBy(columns);
        return this;
    }

    @Override
    public CreateTable zone(String name) {
        createTable.zone(name);
        return this;
    }

    @Override
    public CreateTable recordView(Class<?> recCls) {
        processColumns(createTable, pkType, recCls);
        return this;
    }

    @Override
    public CreateTable keyValueView(Class<?> key, Class<?> value) {
        processColumns(createTable, pkType, key);
        processColumns(createTable, pkType, value);
        return this;
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
