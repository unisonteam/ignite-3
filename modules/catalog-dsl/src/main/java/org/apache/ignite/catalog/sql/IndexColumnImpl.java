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

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.catalog.IndexColumn;
import org.apache.ignite.catalog.SortOrder;

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
        var col = IndexColumn.ix(columnName);

        if (containsAll(lower, "asc", "nulls", "first")) {
            col.sort(SortOrder.ASC_NULLS_FIRST);
        } else if (containsAll(lower, "asc", "nulls", "last")) {
            col.sort(SortOrder.ASC_NULLS_LAST);
        } else if (containsAll(lower, "desc", "nulls", "first")) {
            col.sort(SortOrder.DESC_NULLS_FIRST);
        } else if (containsAll(lower, "desc", "nulls", "last")) {
            col.sort(SortOrder.DESC_NULLS_LAST);
        } else if (lower.contains("asc")) {
            col.sort(SortOrder.ASC);
        } else if (lower.contains("desc")) {
            col.sort(SortOrder.DESC);
        } else if (containsAll(lower, "nulls", "first")) {
            col.sort(SortOrder.NULLS_FIRST);
        } else if (containsAll(lower, "nulls", "last")) {
            col.sort(SortOrder.NULLS_LAST);
        } else {
            col.sort(SortOrder.DEFAULT);
        }
        return col;
    }

    private static boolean containsAll(String in, String... keywords) {
        for (String k : keywords) {
            if (!in.contains(k)) {
                return false;
            }
        }
        return true;
    }

    @Override
    protected void accept(QueryContext ctx) {
        ctx.visit(new Name(wrapped.getColumnName()));
        if (wrapped.getSortOrder() != SortOrder.DEFAULT) {
            var sortOrderStr = wrapped.getSortOrder().name().replaceAll("_", " ").toLowerCase();
            ctx.sql(" ").sql(sortOrderStr);
        }
    }
}
