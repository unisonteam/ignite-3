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

import static org.apache.ignite.catalog.ColumnDefinition.column;
import static org.apache.ignite.catalog.ColumnSorted.column;
import static org.apache.ignite.catalog.ColumnType.INTEGER;
import static org.apache.ignite.catalog.ColumnType.VARCHAR;
import static org.apache.ignite.catalog.SortOrder.DESC_NULLS_LAST;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

import java.util.Objects;
import org.apache.ignite.catalog.IndexType;
import org.apache.ignite.catalog.Options;
import org.apache.ignite.catalog.TableDefinition;
import org.apache.ignite.catalog.ZoneDefinition;
import org.apache.ignite.catalog.ZoneEngine;
import org.apache.ignite.catalog.annotations.Column;
import org.apache.ignite.catalog.annotations.Id;
import org.apache.ignite.catalog.sql.CreateFromAnnotationsTest.Pojo;
import org.junit.jupiter.api.Test;

@SuppressWarnings("LongLine")
class CreateFromDefinitionTest {

    private static final Options quoteIdentifiers = Options.defaultOptions().prettyPrint(false).quoteIdentifiers(true);

    private static CreateFromDefinitionImpl createTable() {
        return createTable(Options.defaultOptions().prettyPrint(false).quoteIdentifiers(false));
    }

    private static CreateFromDefinitionImpl createTable(Options options) {
        return new CreateFromDefinitionImpl(null, options);
    }

    @Test
    void testCreateFromBuilder() {
        var zone = ZoneDefinition.builder("zone_test")
                .ifNotExists()
                .partitions(3)
                .replicas(3)
                .engine(ZoneEngine.AIMEM)
                .build();
        var sql = createTable().from(zone).getSql();
        assertThat(sql, containsString("CREATE ZONE IF NOT EXISTS zone_test ENGINE AIMEM WITH PARTITIONS=3, REPLICAS=3;"));

        var table = TableDefinition.builder("builder_test")
                .ifNotExists()
                .colocateBy("id", "id_str")
                .zone("zone_test")
                .columns(
                        column("id", INTEGER),
                        column("id_str", VARCHAR),
                        column("f_name", VARCHAR(20).notNull().defaultValue("a"))
                )
                .primaryKey("id", "id_str")
                .index("id_str", "f_name")
                .index("ix_test", IndexType.TREE, column("id_str").asc(), column("f_name").sort(DESC_NULLS_LAST))
                .build();
        sql = createTable().from(table).getSql();
        assertThat(sql, containsString("CREATE TABLE IF NOT EXISTS builder_test (id int, id_str varchar, f_name varchar(20) NOT NULL DEFAULT 'a', PRIMARY KEY (id, id_str)) COLOCATE BY (id, id_str) WITH PRIMARY_ZONE='ZONE_TEST';"));
        assertThat(sql, containsString("CREATE INDEX IF NOT EXISTS ix_id_str_f_name ON builder_test (id_str, f_name);"));
        assertThat(sql, containsString("CREATE INDEX IF NOT EXISTS ix_test ON builder_test USING TREE (id_str asc, f_name desc nulls last);"));

        sql = createTable(quoteIdentifiers).from(table).getSql();
        assertThat(sql, containsString("CREATE TABLE IF NOT EXISTS \"builder_test\" (\"id\" int, \"id_str\" varchar, \"f_name\" varchar(20) NOT NULL DEFAULT 'a', PRIMARY KEY (\"id\", \"id_str\")) COLOCATE BY (\"id\", \"id_str\") WITH PRIMARY_ZONE='ZONE_TEST';"));
        assertThat(sql, containsString("CREATE INDEX IF NOT EXISTS \"ix_id_str_f_name\" ON \"builder_test\" (\"id_str\", \"f_name\");"));
        assertThat(sql, containsString("CREATE INDEX IF NOT EXISTS \"ix_test\" ON \"builder_test\" USING TREE (\"id_str\" asc, \"f_name\" desc nulls last);"));
    }

    @Test
    void testCreateFromKeyValueView() {
        // primitive/boxed key class is a primary key with default name 'id'
        var sql = createTable().from(TableDefinition.builder("pojo_value_test")
                        .ifNotExists()
                        .colocateBy("id", "id_str")
                        .zone("zone_test")
                        .keyValueView(Integer.class, PojoValue.class)
                        .build())
                .getSql();
        assertThat(sql, containsString("CREATE TABLE IF NOT EXISTS pojo_value_test (id int, f_name varchar, l_name varchar, str varchar, PRIMARY KEY (id)) COLOCATE BY (id, id_str) WITH PRIMARY_ZONE='ZONE_TEST';"));

        // key class fields (annotated only) is a composite primary keys
        sql = createTable().from(TableDefinition.builder("pojo_value_test")
                        .ifNotExists()
                        .colocateBy("id", "id_str")
                        .zone("zone_test")
                        .keyValueView(PojoKey.class, PojoValue.class)
                        .build())
                .getSql();
        assertThat(sql, containsString("CREATE TABLE IF NOT EXISTS pojo_value_test (id int, id_str varchar(20), f_name varchar, l_name varchar, str varchar, PRIMARY KEY (id, id_str)) COLOCATE BY (id, id_str) WITH PRIMARY_ZONE='ZONE_TEST';"));
    }

    @Test
    void testCreateFromRecordView() {
        var sql = createTable().from(TableDefinition.builder("pojo_test")
                        .ifNotExists()
                        .colocateBy("id", "id_str")
                        .zone("zone_test")
                        .recordView(Pojo.class)
                        .build())
                .getSql();
        assertThat(sql, containsString("CREATE TABLE IF NOT EXISTS pojo_test (id int, id_str varchar(20), f_name varchar(20) not null default 'a', l_name varchar, str varchar, PRIMARY KEY (id, id_str)) COLOCATE BY (id, id_str) WITH PRIMARY_ZONE='ZONE_TEST';"));

        // quote identifiers
        sql = createTable(quoteIdentifiers).from(TableDefinition.builder("pojo_test")
                        .ifNotExists()
                        .colocateBy("id", "id_str")
                        .zone("zone_test")
                        .recordView(Pojo.class)
                        .build())
                .getSql();
        assertThat(sql, containsString("CREATE TABLE IF NOT EXISTS \"pojo_test\" (\"id\" int, \"id_str\" varchar(20), \"f_name\" varchar(20) not null default 'a', \"l_name\" varchar, \"str\" varchar, PRIMARY KEY (\"id\", \"id_str\")) COLOCATE BY (\"id\", \"id_str\") WITH PRIMARY_ZONE='ZONE_TEST';"));
    }

    static class PojoKey {

        @Id
        Integer id;

        @Id
        @Column(name = "id_str", length = 20)
        String idStr;

        PojoKey() {}

        PojoKey(Integer id, String idStr) {
            this.id = id;
            this.idStr = idStr;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PojoKey pojoKey = (PojoKey) o;
            return Objects.equals(id, pojoKey.id) && Objects.equals(idStr, pojoKey.idStr);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, idStr);
        }
    }

    static class PojoValue {
        @Column(name = "f_name")
        String firstName;

        @Column(name = "l_name")
        String lastName;

        String str;

        PojoValue(String firstName, String lastName, String str) {
            this.firstName = firstName;
            this.lastName = lastName;
            this.str = str;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PojoValue pojoValue = (PojoValue) o;
            return Objects.equals(firstName, pojoValue.firstName) && Objects.equals(lastName, pojoValue.lastName)
                    && Objects.equals(str, pojoValue.str);
        }

        @Override
        public int hashCode() {
            return Objects.hash(firstName, lastName, str);
        }
    }
}
