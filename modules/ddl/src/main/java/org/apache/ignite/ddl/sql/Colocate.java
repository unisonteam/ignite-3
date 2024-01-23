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

import static org.apache.ignite.ddl.sql.QueryPartCollection.wrap;

import java.util.List;

class Colocate extends QueryPart {

    private final List<Name> names;

    Colocate(String... columns) {
        names = map(columns, Name::new);
    }

    @Override
    protected void accept(QueryContext ctx) {
        if (names != null) {
            ctx.sql("COLOCATE BY (");
            ctx.visit(wrap(names).separator(", "));
            ctx.sql(")");
        }
    }
}
