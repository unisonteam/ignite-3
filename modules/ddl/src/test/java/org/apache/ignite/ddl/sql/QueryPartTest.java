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

import static org.apache.ignite.ddl.sql.ColumnType.BIGINT;
import static org.apache.ignite.ddl.sql.ColumnType.BOOLEAN;
import static org.apache.ignite.ddl.sql.ColumnType.DATE;
import static org.apache.ignite.ddl.sql.ColumnType.DECIMAL;
import static org.apache.ignite.ddl.sql.ColumnType.DOUBLE;
import static org.apache.ignite.ddl.sql.ColumnType.FLOAT;
import static org.apache.ignite.ddl.sql.ColumnType.INT16;
import static org.apache.ignite.ddl.sql.ColumnType.INT32;
import static org.apache.ignite.ddl.sql.ColumnType.INT64;
import static org.apache.ignite.ddl.sql.ColumnType.INT8;
import static org.apache.ignite.ddl.sql.ColumnType.INTEGER;
import static org.apache.ignite.ddl.sql.ColumnType.REAL;
import static org.apache.ignite.ddl.sql.ColumnType.SMALLINT;
import static org.apache.ignite.ddl.sql.ColumnType.TIME;
import static org.apache.ignite.ddl.sql.ColumnType.TIMESTAMP;
import static org.apache.ignite.ddl.sql.ColumnType.TINYINT;
import static org.apache.ignite.ddl.sql.ColumnType.UUID;
import static org.apache.ignite.ddl.sql.ColumnType.VARBINARY;
import static org.apache.ignite.ddl.sql.ColumnType.VARCHAR;
import static org.apache.ignite.ddl.sql.ColumnTypeImpl.wrap;
import static org.apache.ignite.ddl.sql.IndexColumn.col;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;

import java.math.BigDecimal;
import java.util.List;
import org.apache.ignite.ddl.Options;
import org.junit.jupiter.api.Test;

class QueryPartTest {

    private static final Options quoteIdentifiers = Options.defaultOptions().prettyPrint(false).quoteIdentifiers(true);

    @Test
    void testNamePart() {
        var sql = sql(new Name("a"));
        assertThat(sql, is("a"));

        sql = sql(new Name("a", "b", "c"));
        assertThat(sql, is("a.b.c"));

        sql = sql(quoteIdentifiers, new Name("a"));
        assertThat(sql, is("\"a\""));

        sql = sql(quoteIdentifiers, new Name("a", "b", "c"));
        assertThat(sql, is("\"a\".\"b\".\"c\""));
    }

    @Test
    void testColocatePart() {
        var sql = sql(new Colocate("a"));
        assertThat(sql, is("COLOCATE BY (a)"));

        sql = sql(new Colocate("a", "b"));
        assertThat(sql, is("COLOCATE BY (a, b)"));

        // quote identifiers
        sql = sql(quoteIdentifiers, new Colocate("a"));
        assertThat(sql, is("COLOCATE BY (\"a\")"));

        sql = sql(quoteIdentifiers, new Colocate("a", "b"));
        assertThat(sql, is("COLOCATE BY (\"a\", \"b\")"));
    }

    @Test
    void testColumnPart() {
        var sql = sql(new Column("a", wrap(VARCHAR)));
        assertThat(sql, is("a varchar"));

        sql = sql(new Column("a", wrap(VARCHAR(3))));
        assertThat(sql, is("a varchar(3)"));

        sql = sql(new Column("a", wrap(DECIMAL(2, 3))));
        assertThat(sql, is("a decimal(2, 3)"));

        // quote identifiers
        sql = sql(quoteIdentifiers, new Column("a", wrap(VARCHAR)));
        assertThat(sql, is("\"a\" varchar"));

        sql = sql(quoteIdentifiers, new Column("a", wrap(VARCHAR(3))));
        assertThat(sql, is("\"a\" varchar(3)"));

        sql = sql(quoteIdentifiers, new Column("a", wrap(DECIMAL(2, 3))));
        assertThat(sql, is("\"a\" decimal(2, 3)"));
    }

    @Test
    void testColumnTypePart() {
        var sql = sql(wrap(BOOLEAN));
        assertThat(sql, is("boolean"));

        sql = sql(wrap(TINYINT));
        assertThat(sql, is("tinyint"));

        sql = sql(wrap(SMALLINT));
        assertThat(sql, is("smallint"));

        sql = sql(wrap(INT8));
        assertThat(sql, is("tinyint"));

        sql = sql(wrap(INT16));
        assertThat(sql, is("smallint"));

        sql = sql(wrap(INT32));
        assertThat(sql, is("int"));

        sql = sql(wrap(INT64));
        assertThat(sql, is("bigint"));

        sql = sql(wrap(INTEGER));
        assertThat(sql, is("int"));

        sql = sql(wrap(BIGINT));
        assertThat(sql, is("bigint"));

        sql = sql(wrap(REAL));
        assertThat(sql, is("real"));

        sql = sql(wrap(FLOAT));
        assertThat(sql, is("real"));

        sql = sql(wrap(DOUBLE));
        assertThat(sql, is("double"));

        sql = sql(wrap(VARCHAR));
        assertThat(sql, is("varchar"));

        sql = sql(wrap(VARCHAR(1)));
        assertThat(sql, is("varchar(1)"));

        sql = sql(wrap(VARBINARY));
        assertThat(sql, is("varbinary"));

        sql = sql(wrap(VARBINARY(1)));
        assertThat(sql, is("varbinary(1)"));

        sql = sql(wrap(TIME));
        assertThat(sql, is("time"));

        sql = sql(wrap(TIME(1)));
        assertThat(sql, is("time(1)"));

        sql = sql(wrap(TIMESTAMP));
        assertThat(sql, is("timestamp"));

        sql = sql(wrap(TIMESTAMP(1)));
        assertThat(sql, is("timestamp(1)"));

        sql = sql(wrap(DATE));
        assertThat(sql, is("date"));

        sql = sql(wrap(DECIMAL));
        assertThat(sql, is("decimal"));

        sql = sql(wrap(DECIMAL(1, 2)));
        assertThat(sql, is("decimal(1, 2)"));

        sql = sql(wrap(UUID));
        assertThat(sql, is("uuid"));
    }

    @Test
    void testColumnTypeOptionsPart() {
        var sql = sql(wrap(INTEGER));
        assertThat(sql, is("int"));

        sql = sql(wrap(INTEGER.notNull()));
        assertThat(sql, is("int NOT NULL"));

        sql = sql(wrap(INTEGER.defaultValue(1)));
        assertThat(sql, is("int DEFAULT 1"));

        sql = sql(wrap(VARCHAR.defaultValue("s")));
        assertThat(sql, is("varchar DEFAULT 's'")); // default in single quotes

        sql = sql(wrap(INTEGER.defaultExpression("gen_expr")));
        assertThat(sql, is("int DEFAULT gen_expr"));

        sql = sql(wrap(INTEGER.notNull().defaultValue(1)));
        assertThat(sql, is("int NOT NULL DEFAULT 1"));

        sql = sql(wrap(DECIMAL(2, 3).defaultValue(BigDecimal.ONE).notNull()));
        assertThat(sql, is("decimal(2, 3) NOT NULL DEFAULT 1"));

        sql = sql(wrap(INTEGER.defaultValue(1).defaultExpression("gen_expr"))); // default value takes priority
        assertThat(sql, is("int DEFAULT 1"));
    }

    @Test
    void testConstraintsPart() {
        var sql = sql(new Constraint().primaryKey(col("a")));
        assertThat(sql, is("PRIMARY KEY (a)"));

        sql = sql(new Constraint().primaryKey(col("a"), col("b")));
        assertThat(sql, is("PRIMARY KEY (a, b)"));

        // quote identifiers
        sql = sql(quoteIdentifiers, new Constraint().primaryKey(col("a")));
        assertThat(sql, is("PRIMARY KEY (\"a\")"));

        sql = sql(quoteIdentifiers, new Constraint().primaryKey(col("a"), col("b")));
        assertThat(sql, is("PRIMARY KEY (\"a\", \"b\")"));
    }

    @Test
    void testWithOptionPart() {
        var sql = sql(WithOption.primaryZone("z"));
        assertThat(sql, is("PRIMARY_ZONE='Z'"));

        sql = sql(WithOption.partitions(1));
        assertThat(sql, is("PARTITIONS=1"));

        sql = sql(WithOption.replicas(1));
        assertThat(sql, is("REPLICAS=1"));

        sql = sql(WithOption.affinityFunction("f"));
        assertThat(sql, is("AFFINITY_FUNCTION='f'"));

        // quote identifiers
        sql = sql(quoteIdentifiers, WithOption.primaryZone("z"));
        assertThat(sql, is("PRIMARY_ZONE='Z'"));

        sql = sql(quoteIdentifiers, WithOption.partitions(1));
        assertThat(sql, is("PARTITIONS=1"));

        sql = sql(quoteIdentifiers, WithOption.replicas(1));
        assertThat(sql, is("REPLICAS=1"));

        sql = sql(quoteIdentifiers, WithOption.affinityFunction("f"));
        assertThat(sql, is("AFFINITY_FUNCTION='f'"));
    }

    @Test
    void testQueryPartCollection() {
        var names = List.of(new Name("a"), new Name("b"));
        var wrapped = QueryPartCollection.wrap(names).separator(", ");

        var sql = sql(wrapped);
        assertThat(sql, is("a, b"));

        // quote identifiers
        sql = sql(quoteIdentifiers, wrapped);
        assertThat(sql, is("\"a\", \"b\""));
    }

    @Test
    void testIndexColumnPart() {
        var col = col("col1");
        var sql = sql(IndexColumnImpl.wrap(col));
        assertThat(sql, is("col1"));

        col = col("col1").asc().nullsFirst();
        sql = sql(IndexColumnImpl.wrap(col));
        assertThat(sql, is("col1 asc nulls first"));

        col = col("col1").nullsLast().desc();
        sql = sql(IndexColumnImpl.wrap(col));
        assertThat(sql, is("col1 desc nulls last"));

        // quote identifiers
        col = col("col1");
        sql = sql(quoteIdentifiers, IndexColumnImpl.wrap(col));
        assertThat(sql, is("\"col1\""));

        col = col("col1").asc().nullsFirst();
        sql = sql(quoteIdentifiers, IndexColumnImpl.wrap(col));
        assertThat(sql, is("\"col1\" asc nulls first"));

        col = col("col1").nullsLast().desc();
        sql = sql(quoteIdentifiers, IndexColumnImpl.wrap(col));
        assertThat(sql, is("\"col1\" desc nulls last"));
    }

    @Test
    void testIndexColumnPartParse() {
        var cols = IndexColumnImpl.parseIndexColumnList("col1");
        assertThat(cols, containsInAnyOrder(col("col1")));

        cols = IndexColumnImpl.parseIndexColumnList("col1, COL_UPPER_CASE ASC, col3 nulls first, col4 desc nulls last");
        assertThat(cols, containsInAnyOrder(
                col("col1"),
                col("COL_UPPER_CASE").asc(),
                col("col3").nullsFirst(),
                col("col4").desc().nullsLast()
        ));

        cols = IndexColumnImpl.parseIndexColumnList("col1 unexpectedKeyword");
        assertThat(cols, containsInAnyOrder(col("col1")));
    }

    private static String sql(QueryPart part) {
        return ctx().visit(part).getSql();
    }

    private static String sql(Options options, QueryPart part) {
        return ctx(options).visit(part).getSql();
    }

    private static QueryContext ctx() {
        return ctx(Options.defaultOptions());
    }

    private static QueryContext ctx(Options options) {
        return new QueryContext(options);
    }

}