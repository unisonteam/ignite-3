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

package org.apache.ignite.catalog;

import java.util.Objects;

public class IndexColumn {

    private final String columnName;

    private SortOrder sortOrder = SortOrder.DEFAULT;

    private IndexColumn(String columnName) {
        Objects.requireNonNull(columnName, "index column name is null");
        this.columnName = columnName;
    }

    public static IndexColumn ix(String name) {
        return new IndexColumn(name);
    }

    public static IndexColumn ix(String name, SortOrder sortOrder) {
        return new IndexColumn(name).sort(sortOrder);
    }

    public IndexColumn sort(SortOrder sortOrder) {
        this.sortOrder = sortOrder;
        return this;
    }

    public String getColumnName() {
        return columnName;
    }

    public SortOrder getSortOrder() {
        return sortOrder;
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
        return Objects.equals(columnName, that.columnName) && sortOrder == that.sortOrder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(columnName, sortOrder);
    }
}
