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

import java.util.ArrayList;
import java.util.List;

class IndexColumnImpl extends QueryPart {

    private final IndexColumn wrapped;

    public static IndexColumnImpl wrap(IndexColumn column) {
        return new IndexColumnImpl(column);
    }

    private IndexColumnImpl(IndexColumn wrapped) {
        this.wrapped = wrapped;
    }

    public static List<IndexColumn> parseIndexColumnList(String columnList) {
        var result = new ArrayList<IndexColumn>();
        for (var s : columnList.split("\\s*,\\s*")) {
            result.add(parseCol(s));
        }
        return result;
    }

    private static IndexColumn parseCol(String columnRaw) {
        String columnName = columnRaw.split("\\s+", 2)[0];
        var lower = columnRaw.toLowerCase();
        var result = IndexColumn.col(columnName);

        if (lower.contains("asc")) {
            result.asc();
        }
        if (lower.contains("desc")) {
            result.desc();
        }
        if (lower.contains("nulls first")) {
            result.nullsFirst();
        }
        if (lower.contains("nulls last")) {
            result.nullsLast();
        }
        return result;
    }

    @Override
    protected void accept(QueryContext ctx) {
        ctx.visit(new Name(wrapped.getColumnName()));
        if (wrapped.getSortOrder() != null) {
            ctx.sql(" ").sql(wrapped.getSortOrder());
        }
        if (wrapped.getNullsOrder() != null) {
            ctx.sql(" ").sql(wrapped.getNullsOrder());
        }
    }
}
