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

public class ColumnSorted {

    private final String columnName;

    private SortOrder sortOrder = SortOrder.DEFAULT;

    private ColumnSorted(String columnName) {
        Objects.requireNonNull(columnName, "index column name is null");
        this.columnName = columnName;
    }

    public static ColumnSorted column(String name) {
        return new ColumnSorted(name);
    }

    public static ColumnSorted column(String name, SortOrder sortOrder) {
        return new ColumnSorted(name).sort(sortOrder);
    }

    public ColumnSorted asc() {
        return sort(SortOrder.ASC);
    }

    public ColumnSorted desc() {
        return sort(SortOrder.DESC);
    }

    public ColumnSorted sort(SortOrder sortOrder) {
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
        ColumnSorted that = (ColumnSorted) o;
        return Objects.equals(columnName, that.columnName) && sortOrder == that.sortOrder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(columnName, sortOrder);
    }
}
