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

import org.apache.ignite.catalog.IgniteCatalog;
import org.apache.ignite.catalog.Options;
import org.apache.ignite.catalog.Query;
import org.apache.ignite.catalog.TableDefinition;
import org.apache.ignite.catalog.ZoneDefinition;
import org.apache.ignite.sql.IgniteSql;

public class IgniteCatalogSqlImpl implements IgniteCatalog {

    private final IgniteSql sql;
    private final Options options;

    public IgniteCatalogSqlImpl(IgniteSql sql, Options options) {
        this.options = options;
        this.sql = sql;
    }

    @Override
    public Query create(Class<?> key, Class<?> value) {
        return new CreateFromAnnotationsImpl(sql, options).ifNotExists().keyValueView(key, value);
    }

    @Override
    public Query create(Class<?> recCls) {
        return new CreateFromAnnotationsImpl(sql, options).ifNotExists().recordView(recCls);
    }

    @Override
    public Query create(TableDefinition definition) {
        return new CreateFromDefinitionImpl(sql, options).from(definition);
    }

    @Override
    public Query create(ZoneDefinition definition) {
        return new CreateFromDefinitionImpl(sql, options).from(definition);
    }

    @Override
    public Query drop(TableDefinition definition) {
        return new DropTableImpl(sql, options).name(definition.getSchemaName(), definition.getTableName()).ifExists();
    }

    @Override
    public Query drop(ZoneDefinition definition) {
        return new DropZoneImpl(sql, options).name(definition.getZoneName()).ifExists();
    }

    @Override
    public Query dropTable(String name) {
        return new DropTableImpl(sql, options).name(name).ifExists();
    }

    @Override
    public Query dropZone(String name) {
        return new DropZoneImpl(sql, options).name(name).ifExists();
    }
}
