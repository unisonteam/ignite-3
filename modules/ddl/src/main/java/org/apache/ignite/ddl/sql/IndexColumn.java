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

class IndexColumn {

    private final String columnName;

    private String sortOrder;

    private String nullsOrder;

    private IndexColumn(String columnName) {
        Objects.requireNonNull(columnName, "index column name is null");
        this.columnName = columnName;
    }

    public static IndexColumn col(String name) {
        return new IndexColumn(name);
    }

    public IndexColumn asc() {
        this.sortOrder = "asc";
        return this;
    }

    public IndexColumn desc() {
        this.sortOrder = "desc";
        return this;
    }

    public IndexColumn nullsFirst() {
        this.nullsOrder = "nulls first";
        return this;
    }

    public IndexColumn nullsLast() {
        this.nullsOrder = "nulls last";
        return this;
    }

    public String getColumnName() {
        return columnName;
    }

    public String getSortOrder() {
        return sortOrder;
    }

    public String getNullsOrder() {
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
