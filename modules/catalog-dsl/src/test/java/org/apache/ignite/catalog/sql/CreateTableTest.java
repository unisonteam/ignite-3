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

import static org.apache.ignite.catalog.ColumnSorted.column;
import static org.apache.ignite.catalog.ColumnType.DECIMAL;
import static org.apache.ignite.catalog.ColumnType.INTEGER;
import static org.apache.ignite.catalog.ColumnType.UUID;
import static org.apache.ignite.catalog.ColumnType.VARCHAR;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.is;

import org.apache.ignite.catalog.IndexType;
import org.apache.ignite.catalog.Options;
import org.apache.ignite.catalog.SortOrder;
import org.junit.jupiter.api.Test;

class CreateTableTest {

    private static final Options quoteIdentifiers = Options.defaultOptions().prettyPrint(false).quoteIdentifiers(true);

    private static CreateTableImpl createTable() {
        return createTable(Options.defaultOptions().prettyPrint(false).quoteIdentifiers(false));
    }

    private static CreateTableImpl createTable(Options options) {
        return new CreateTableImpl(null, options);
    }


    @Test
    void testIfNotExists() {
        var sql = createTable().ifNotExists().name("table1").getSql();
        assertThat(sql, is("CREATE TABLE IF NOT EXISTS table1 ();"));

        sql = createTable(quoteIdentifiers).ifNotExists().name("table1").getSql();
        assertThat(sql, is("CREATE TABLE IF NOT EXISTS \"table1\" ();"));
    }

    @Test
    void testNames() {
        var sql = createTable().name("public", "table1").getSql();
        assertThat(sql, is("CREATE TABLE public.table1 ();"));

        sql = createTable().name("", "table;1--test\n\r\t;").getSql();
        assertThat(sql, is("CREATE TABLE table1 ();"));

        // quote identifiers
        sql = createTable(quoteIdentifiers).name("public", "table1").getSql();
        assertThat(sql, is("CREATE TABLE \"public\".\"table1\" ();"));

        sql = createTable(quoteIdentifiers).name("", "table;1--test\n\r\t;").getSql();
        assertThat(sql, is("CREATE TABLE \"table1\" ();"));
    }

    @Test
    void testColumns() {
        var sql = createTable().name("table1")
                .column("col", INTEGER).getSql();
        assertThat(sql, is("CREATE TABLE table1 (col int);"));

        sql = createTable().name("table1")
                .column("col1", INTEGER)
                .column("col2", INTEGER)
                .getSql();
        assertThat(sql, is("CREATE TABLE table1 (col1 int, col2 int);"));

        // quote identifiers
        sql = createTable(quoteIdentifiers).name("table1")
                .column("col", INTEGER).getSql();
        assertThat(sql, is("CREATE TABLE \"table1\" (\"col\" int);"));

        sql = createTable(quoteIdentifiers).name("table1")
                .column("col1", INTEGER)
                .column("col2", INTEGER)
                .getSql();
        assertThat(sql, is("CREATE TABLE \"table1\" (\"col1\" int, \"col2\" int);"));
    }

    @Test
    void testPrimaryKey() {
        var sql = createTable().name("table1")
                .column("col", INTEGER)
                .primaryKey("col")
                .getSql();
        assertThat(sql, is("CREATE TABLE table1 (col int, PRIMARY KEY (col));"));

        sql = createTable().name("table1")
                .column("col1", INTEGER)
                .column("col2", INTEGER)
                .primaryKey("col1, col2")
                .getSql();
        assertThat(sql, is("CREATE TABLE table1 (col1 int, col2 int, PRIMARY KEY (col1, col2));"));

        sql = createTable().name("table1")
                .column("col1", INTEGER)
                .primaryKey(IndexType.TREE, "col1 nUlls First   ASC")
                .getSql();
        assertThat(sql, is("CREATE TABLE table1 (col1 int, PRIMARY KEY USING TREE (col1 asc nulls first));"));

        // quote identifiers
        sql = createTable(quoteIdentifiers).name("table1")
                .column("col", INTEGER)
                .primaryKey("col")
                .getSql();
        assertThat(sql, is("CREATE TABLE \"table1\" (\"col\" int, PRIMARY KEY (\"col\"));"));

        sql = createTable(quoteIdentifiers).name("table1")
                .column("col1", INTEGER)
                .column("col2", INTEGER)
                .primaryKey("col1, col2")
                .getSql();
        assertThat(sql, is("CREATE TABLE \"table1\" (\"col1\" int, \"col2\" int, PRIMARY KEY (\"col1\", \"col2\"));"));

        sql = createTable(quoteIdentifiers).name("table1")
                .column("col1", INTEGER)
                .primaryKey(IndexType.TREE, "col1 nUlls First   ASC")
                .getSql();
        assertThat(sql, is("CREATE TABLE \"table1\" (\"col1\" int, PRIMARY KEY USING TREE (\"col1\" asc nulls first));"));
    }

    @Test
    void testColocateBy() {
        var sql = createTable().name("table1").colocateBy("col1").getSql();
        assertThat(sql, is("CREATE TABLE table1 () COLOCATE BY (col1);"));

        sql = createTable().name("table1").colocateBy("col1", "col2").getSql();
        assertThat(sql, is("CREATE TABLE table1 () COLOCATE BY (col1, col2);"));

        // quote identifiers
        sql = createTable(quoteIdentifiers).name("table1").colocateBy("col1").getSql();
        assertThat(sql, is("CREATE TABLE \"table1\" () COLOCATE BY (\"col1\");"));

        sql = createTable(quoteIdentifiers).name("table1").colocateBy("col1", "col2").getSql();
        assertThat(sql, is("CREATE TABLE \"table1\" () COLOCATE BY (\"col1\", \"col2\");"));
    }

    @Test
    void testWithOptions() {
        var sql = createTable().name("table1").zone("zone1").getSql(); // zone param is lowercase
        assertThat(sql, is("CREATE TABLE table1 () WITH PRIMARY_ZONE='ZONE1';")); // zone result is uppercase

        sql = createTable(quoteIdentifiers).name("table1").zone("zone1").getSql();
        assertThat(sql, is("CREATE TABLE \"table1\" () WITH PRIMARY_ZONE='ZONE1';"));
    }

    @Test
    void testIndex() {
        var sql = createTable().name("table1")
                .index("ix_test1", "col1, COL2_UPPER desc nulls last").getSql();
        assertThat(sql, endsWith("CREATE INDEX IF NOT EXISTS ix_test1 ON table1 (col1, COL2_UPPER desc nulls last);"));

        sql = createTable().name("table1")
                .index("ix_test1", IndexType.HASH, "col1").getSql();
        assertThat(sql, endsWith("CREATE INDEX IF NOT EXISTS ix_test1 ON table1 USING HASH (col1);"));

        // quote identifiers
        sql = createTable(quoteIdentifiers).name("table1")
                .index("ix_test1", "col1, COL2_UPPER desc nulls last").getSql();
        assertThat(sql, endsWith("CREATE INDEX IF NOT EXISTS \"ix_test1\" ON \"table1\" (\"col1\", \"COL2_UPPER\" desc nulls last);"));

        sql = createTable(quoteIdentifiers).name("table1")
                .index("ix_test1", IndexType.HASH, "col1").getSql();
        assertThat(sql, endsWith("CREATE INDEX IF NOT EXISTS \"ix_test1\" ON \"table1\" USING HASH (\"col1\");"));
    }

    @Test
    void testCreateTableFluentBuilder() {
        var createTable = createTable().name("public", "table1")
                .column("col", INTEGER)
                .column("col", INTEGER.defaultValue(1))
                .column("col", INTEGER.defaultExpression("gen_smth"))
                .column("col", DECIMAL)
                .column("col", DECIMAL(2, 3))
                .column("col", VARCHAR)
                .column("col", VARCHAR(36).notNull())
                .column("col", VARCHAR(36).defaultValue("default1"))
                .column("col", VARCHAR(36).notNull().defaultValue("default1"))
                .column("col", VARCHAR(36).defaultValue("default1").notNull())
                .column("col", UUID.notNull().defaultExpression("gen_random_uuid"))
                .column("col", UUID.defaultValue(java.util.UUID.randomUUID()).defaultExpression("gen_random_uuid"))
                .primaryKey("col1, col2, col3")
                .colocateBy("col1", "col2", "col3")
                .zone("zone1")
                .index("ix_test1", "col1, col2 asc, col3 desc nulls first")
                .index("ix_test2", IndexType.HASH, column("col1").asc(), column("col2").sort(SortOrder.DESC_NULLS_LAST))
                .getSql();
        System.out.println(createTable);
    }

}