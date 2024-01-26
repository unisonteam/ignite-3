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

import java.util.Objects;
import org.apache.ignite.ddl.SortOrder;

class IndexColumn {

    private final String columnName;

    private String sortOrder;

    private String nullsOrder;

    private IndexColumn(String columnName) {
        Objects.requireNonNull(columnName, "index column name is null");
        this.columnName = columnName;
    }

    static IndexColumn col(String name) {
        return new IndexColumn(name);
    }

    static IndexColumn col(String name, SortOrder sortOrder) {
        return new IndexColumn(name).sort(sortOrder);
    }

    IndexColumn sort(SortOrder sortOrder) {
        switch (sortOrder) {
            case DEFAULT:
                break;
            case ASC:
                this.asc();
                break;
            case ASC_NULLS_FIRST:
                this.asc().nullsFirst();
                break;
            case ASC_NULLS_LAST:
                this.asc().nullsLast();
                break;
            case DESC:
                this.desc();
                break;
            case DESC_NULLS_FIRST:
                this.desc().nullsFirst();
                break;
            case DESC_NULLS_LAST:
                this.desc().nullsLast();
                break;
            default:
                throw new IllegalStateException("Unexpected sort order: " + sortOrder);
        }
        return this;
    }

    IndexColumn asc() {
        this.sortOrder = "asc";
        return this;
    }

    IndexColumn desc() {
        this.sortOrder = "desc";
        return this;
    }

    IndexColumn nullsFirst() {
        this.nullsOrder = "nulls first";
        return this;
    }

    IndexColumn nullsLast() {
        this.nullsOrder = "nulls last";
        return this;
    }

    String getColumnName() {
        return columnName;
    }

    String getSortOrder() {
        return sortOrder;
    }

    String getNullsOrder() {
        return nullsOrder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IndexColumn that = (IndexColumn) o;
        return Objects.equals(columnName, that.columnName)
                && Objects.equals(sortOrder, that.sortOrder)
                && Objects.equals(nullsOrder, that.nullsOrder);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columnName, sortOrder, nullsOrder);
    }
}
