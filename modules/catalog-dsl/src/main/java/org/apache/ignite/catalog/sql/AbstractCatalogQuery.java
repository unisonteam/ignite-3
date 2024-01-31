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

import org.apache.ignite.catalog.Options;
import org.apache.ignite.catalog.Query;
import org.apache.ignite.sql.IgniteSql;

abstract class AbstractCatalogQuery extends QueryPart implements Query {

    protected final IgniteSql sql;
    private final Options options;

    AbstractCatalogQuery(IgniteSql sql, Options options) {
        this.sql = sql;
        this.options = options;
    }

    @Override
    public void execute() {
        //  multi-statement just partially merged IGNITE-20453
        try (var session = sql.createSession()) {
            var queries = getSql().split("\\s*;\\s*");
            for (var query : queries) {
                session.execute(null, query);
            }
        }
    }

    @Override
    public String getSql() {
        return new QueryContext(options())
                .visit(this)
                .getSql();
    }

    @Override
    public String toString() {
        return new QueryContext(Options.defaultOptions().prettyPrint())
                .visit(this)
                .getSql();
    }

    public Options options() {
        return options;
    }
}