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

import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

public class ColumnType<T> {

    /**
     * A cache for Column types by Java type.
     */
    private static final Map<Class<?>, ColumnType<?>> TYPES = new LinkedHashMap<>();

    public static final ColumnType<Boolean> BOOLEAN = new ColumnType<>(Boolean.class, "boolean");

    public static final ColumnType<Byte> TINYINT = new ColumnType<>(Byte.class, "tinyint");

    public static final ColumnType<Short> SMALLINT = new ColumnType<>(Short.class, "smallint");

    public static final ColumnType<Byte> INT8 = new ColumnType<>(Byte.class, "tinyint");

    public static final ColumnType<Short> INT16 = new ColumnType<>(Short.class, "smallint");

    public static final ColumnType<Integer> INT32 = new ColumnType<>(Integer.class, "int");

    public static final ColumnType<Long> INT64 = new ColumnType<>(Long.class, "bigint");

    public static final ColumnType<Integer> INTEGER = new ColumnType<>(Integer.class, "int");

    public static final ColumnType<Long> BIGINT = new ColumnType<>(Long.class, "bigint");

    public static final ColumnType<Float> REAL = new ColumnType<>(Float.class, "real");

    public static final ColumnType<Float> FLOAT = new ColumnType<>(Float.class, "real");

    public static final ColumnType<Double> DOUBLE = new ColumnType<>(Double.class, "double");

    public static final ColumnType<String> VARCHAR = new ColumnType<>(String.class, "varchar");

    @SuppressWarnings("CheckStyle")
    public static final ColumnType<String> VARCHAR(int length) {
        return VARCHAR.copy().length(length);
    }

    public static final ColumnType<byte[]> VARBINARY = new ColumnType<>(byte[].class, "varbinary");

    @SuppressWarnings("CheckStyle")
    public static final ColumnType<byte[]> VARBINARY(int length) {
        return VARBINARY.copy().length(length);
    }

    public static final ColumnType<Time> TIME = new ColumnType<>(Time.class, "time");

    @SuppressWarnings("CheckStyle")
    public static final ColumnType<Time> TIME(int precision) {
        return TIME.copy().precision(precision);
    }

    public static final ColumnType<Timestamp> TIMESTAMP = new ColumnType<>(Timestamp.class, "timestamp");

    @SuppressWarnings("CheckStyle")
    public static final ColumnType<Timestamp> TIMESTAMP(int precision) {
        return TIMESTAMP.copy().precision(precision);
    }

    public static final ColumnType<Date> DATE = new ColumnType<>(Date.class, "date");

    public static final ColumnType<BigDecimal> DECIMAL = new ColumnType<>(BigDecimal.class, "decimal");
    @SuppressWarnings("CheckStyle")
    public static final ColumnType<BigDecimal> DECIMAL(int precision, int scale) {
        return DECIMAL.copy().precision(precision, scale);
    }

    public static final ColumnType<UUID> UUID = new ColumnType<>(UUID.class, "uuid");


    private final Class<T> type;
    private final String typeName;
    private Boolean nullable;
    private T defaultValue;
    private String defaultExpression;
    private Integer precision;
    private Integer scale;
    private Integer length;


    private ColumnType(Class<T> type, String typeName) {
        this.type = type;
        this.typeName = typeName;
        TYPES.putIfAbsent(type, this);
    }

    private ColumnType(ColumnType<T> ref) {
        this.type = ref.type;
        this.typeName = ref.typeName;
        this.precision = ref.precision;
        this.scale = ref.scale;
        this.length = ref.length;
        this.nullable = ref.nullable;
        this.defaultValue = ref.defaultValue;
        this.defaultExpression = ref.defaultExpression;
    }

    public static ColumnType<?> of(Class<?> type, Integer length, Integer precision, Integer scale, Boolean nullable) {
        return of(type)
                .length_(length)
                .precision_(precision)
                .scale_(scale)
                .nullable_(nullable);
    }

    public static ColumnType<?> of(Class<?> type) {
        ColumnType<?> columnType = TYPES.get(type);
        if (columnType == null) {
            throw new UnsupportedOperationException("class is not supported: " + type.getCanonicalName());
        }
        return columnType.copy();
    }

    ColumnType<T> copy() {
        return new ColumnType<>(this);
    }

    public ColumnType<T> notNull() {
        return copy().nullable(false);
    }

    public ColumnType<T> defaultValue(T value) {
        return copy().defaultValue_(value);
    }

    public ColumnType<T> defaultExpression(String expression) {
        return copy().defaultExpression_(expression);
    }

    ColumnType<T> length(Integer length) {
        return copy().length_(length);
    }

    ColumnType<T> precision(Integer precision) {
        return copy().precision_(precision);
    }

    ColumnType<T> precision(Integer precision, Integer scale) {
        return copy().precision_(precision, scale);
    }

    ColumnType<T> scale(Integer scale) {
        return copy().scale_(scale);
    }

    ColumnType<T> nullable(Boolean n) {
        return copy().nullable_(n);
    }

    private ColumnType<T> length_(Integer length) {
        this.length = length;
        return this;
    }

    private ColumnType<T> precision_(Integer precision) {
        this.precision = precision;
        return this;
    }

    private ColumnType<T> precision_(Integer precision, Integer scale) {
        this.precision = precision;
        this.scale = scale;
        return this;
    }

    private ColumnType<T> scale_(Integer scale) {
        this.scale = scale;
        return this;
    }

    private ColumnType<T> nullable_(Boolean n) {
        this.nullable = n;
        return this;
    }

    private ColumnType<T> defaultValue_(T value) {
        this.defaultValue = value;
        return this;
    }

    private ColumnType<T> defaultExpression_(String expression) {
        this.defaultExpression = expression;
        return this;
    }

    public Class<T> getType() {
        return type;
    }

    public String getTypeName() {
        return typeName;
    }

    public Boolean getNullable() {
        return nullable;
    }

    public T getDefaultValue() {
        return defaultValue;
    }

    public String getDefaultExpression() {
        return defaultExpression;
    }

    public Integer getPrecision() {
        return precision;
    }

    public Integer getScale() {
        return scale;
    }

    public Integer getLength() {
        return length;
    }
}
