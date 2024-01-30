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

import org.apache.ignite.ddl.CreateTable;
import org.apache.ignite.ddl.IgniteDdl;
import org.apache.ignite.ddl.Options;
import org.apache.ignite.ddl.Query;
import org.apache.ignite.sql.IgniteSql;

public class IgniteDdlSqlImpl implements IgniteDdl {

    private final IgniteSql sql;
    private final Options options;

    public IgniteDdlSqlImpl(IgniteSql sql, Options options) {
        this.options = options;
        this.sql = sql;
    }

    @Override
    public Query createIfNotExists(Class<?> key, Class<?> value) {
        return new CreateTableFromClassImpl(sql, options).ifNotExists().keyValueView(key, value);
    }

    @Override
    public Query createIfNotExists(Class<?> recCls) {
        return new CreateTableFromClassImpl(sql, options).ifNotExists().recordView(recCls);
    }

    @Override
    public Query dropTableIfExists(String name) {
        return new DropTableImpl(sql, options).name(name).ifExists();
    }

    @Override
    public Query dropIndexIfExists(String name) {
        throw new RuntimeException("NOT YET IMPLEMENTED");
    }

    @Override
    public Query dropZoneIfExists(String name) {
        throw new RuntimeException("NOT YET IMPLEMENTED");
    }

    @Override
    public CreateTable createTable(String name) {
        return new CreateTableFromClassExtendedImpl(sql, options).name(name); // builder alternative for @Table
    }
}
