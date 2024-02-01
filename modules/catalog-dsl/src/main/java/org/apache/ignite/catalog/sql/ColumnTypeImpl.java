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

import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;
import java.util.UUID;
import org.apache.ignite.catalog.ColumnType;

class ColumnTypeImpl<T> extends QueryPart {

    private final ColumnType<T> wrapped;

    public static <T> ColumnTypeImpl<T> wrap(ColumnType<T> type) {
        return new ColumnTypeImpl<>(type);
    }

    private ColumnTypeImpl(ColumnType<T> type) {
        this.wrapped = type;
    }

    @Override
    protected void accept(QueryContext ctx) {
        ctx.sql(wrapped.getTypeName());

        if (isGreaterThanZero(wrapped.getLength())) {
            ctx.sql("(").sql(wrapped.getLength()).sql(")");
        } else if (isGreaterThanZero(wrapped.getPrecision())) {
            if (isGreaterThanZero(wrapped.getScale())) {
                ctx.sql("(").sql(wrapped.getPrecision()).sql(", ").sql(wrapped.getScale()).sql(")");
            } else {
                ctx.sql("(").sql(wrapped.getPrecision()).sql(")");
            }
        }

        if (wrapped.getNullable() != null && !wrapped.getNullable()) {
            ctx.sql(" NOT NULL");
        }

        if (wrapped.getDefaultValue() != null) {
            if (isNeedsQuotes(wrapped)) {
                ctx.sql(" DEFAULT '").sql(wrapped.getDefaultValue().toString()).sql("'");
            } else {
                ctx.sql(" DEFAULT ").sql(wrapped.getDefaultValue().toString());
            }
        } else if (wrapped.getDefaultExpression() != null) {
            ctx.sql(" DEFAULT ").sql(wrapped.getDefaultExpression());
        }
    }

    private static boolean isGreaterThanZero(Integer n) {
        return n != null && n > 0;
    }

    private static boolean isNeedsQuotes(ColumnType<?> type) {
        var typeClass = type.getType();
        return String.class.equals(typeClass)
                || Date.class.equals(typeClass)
                || Time.class.equals(typeClass)
                || Timestamp.class.equals(typeClass)
                || byte[].class.equals(typeClass)
                || UUID.class.equals(typeClass);
    }
}
