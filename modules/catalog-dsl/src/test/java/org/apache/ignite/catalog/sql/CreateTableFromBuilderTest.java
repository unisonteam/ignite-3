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

import static org.apache.ignite.catalog.Column.col;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

import java.util.Objects;
import org.apache.ignite.catalog.Options;
import org.apache.ignite.catalog.annotations.Column;
import org.apache.ignite.catalog.annotations.Id;
import org.apache.ignite.catalog.ColumnType;
import org.apache.ignite.catalog.TableDefinition;
import org.apache.ignite.catalog.sql.CreateTableFromAnnotationsTest.Pojo;
import org.junit.jupiter.api.Test;

@SuppressWarnings("LongLine")
public class CreateTableFromBuilderTest {

    private static final Options quoteIdentifiers = Options.defaultOptions().prettyPrint(false).quoteIdentifiers(true);

    private static CreateTableFromBuilderImpl createTable() {
        return createTable(Options.defaultOptions().prettyPrint(false).quoteIdentifiers(false));
    }

    private static CreateTableFromBuilderImpl createTable(Options options) {
        return new CreateTableFromBuilderImpl(null, options);
    }

    @Test
    void testCreateFromBuilder() {
        var builder = TableDefinition.builder()
                .tableName("builder_test")
                .ifNotExists()
                .colocateBy("id", "id_str")
                .zone("zone_test")
                .columns(
                        col("id", ColumnType.INTEGER),
                        col("id_str", ColumnType.VARCHAR(20)),
                        col("f_name", ColumnType.VARCHAR),
                        col("l_name", ColumnType.VARCHAR),
                        col("str", ColumnType.VARCHAR)
                )
                .primaryKey("id", "id_str")
                .build();
        var sql = createTable().from(builder).getSql();
        assertThat(sql, containsString("CREATE TABLE IF NOT EXISTS builder_test (id int, id_str varchar(20), f_name varchar, l_name varchar, str varchar, PRIMARY KEY (id, id_str)) COLOCATE BY (id, id_str) WITH PRIMARY_ZONE='ZONE_TEST';"));

        sql = createTable(quoteIdentifiers).from(builder).getSql();
        assertThat(sql, containsString("CREATE TABLE IF NOT EXISTS \"builder_test\" (\"id\" int, \"id_str\" varchar(20), \"f_name\" varchar, \"l_name\" varchar, \"str\" varchar, PRIMARY KEY (\"id\", \"id_str\")) COLOCATE BY (\"id\", \"id_str\") WITH PRIMARY_ZONE='ZONE_TEST';"));

    }

    @Test
    void testCreateFromKeyValueView() {
        // primitive/boxed key class is a primary key with default name 'id'
        var sql = createTable().from(TableDefinition.builder()
                        .tableName("pojo_value_test")
                        .ifNotExists()
                        .colocateBy("id", "id_str")
                        .zone("zone_test")
                        .keyValueView(Integer.class, PojoValue.class)
                        .build())
                .getSql();
        assertThat(sql, containsString("CREATE TABLE IF NOT EXISTS pojo_value_test (id int, f_name varchar, l_name varchar, str varchar, PRIMARY KEY (id)) COLOCATE BY (id, id_str) WITH PRIMARY_ZONE='ZONE_TEST';"));

        // key class fields (annotated only) is a composite primary keys
        sql = createTable().from(TableDefinition.builder()
                        .tableName("pojo_value_test")
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
        var sql = createTable().from(TableDefinition.builder()
                        .tableName("pojo_test")
                        .ifNotExists()
                        .colocateBy("id", "id_str")
                        .zone("zone_test")
                        .recordView(Pojo.class)
                        .build())
                .getSql();
        assertThat(sql, containsString("CREATE TABLE IF NOT EXISTS pojo_test (id int, id_str varchar(20), f_name varchar(20) not null default 'a', l_name varchar, str varchar, PRIMARY KEY (id, id_str)) COLOCATE BY (id, id_str) WITH PRIMARY_ZONE='ZONE_TEST';"));

        // quote identifiers
        sql = createTable(quoteIdentifiers).from(TableDefinition.builder()
                        .tableName("pojo_test")
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
