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

/**
 * Qualified SQL identifier.
 */
class Name extends QueryPart {

    private final String[] names;

    /**
     * Identifier name. E.g [db, schema, table] become 'db.schema.table' or '"db"."schema"."table"' depending on DDL options.
     *
     * @param names name parts of qualified identifier.
     */
    Name(String... names) {
        this.names = names;
    }

    @Override
    protected void accept(QueryContext ctx) {
        var c = ctx.isQuoteNames() ? "\"" : "";
        var separator = "";
        for (String name : names) {
            ctx.sql(separator).sql(c).sql(name).sql(c);
            separator = ".";
        }
    }
}
