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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

import java.util.Objects;
import org.apache.ignite.catalog.IndexType;
import org.apache.ignite.catalog.Options;
import org.apache.ignite.catalog.SortOrder;
import org.apache.ignite.catalog.ZoneEngine;
import org.apache.ignite.catalog.annotations.Col;
import org.apache.ignite.catalog.annotations.ColocateBy;
import org.apache.ignite.catalog.annotations.Column;
import org.apache.ignite.catalog.annotations.Id;
import org.apache.ignite.catalog.annotations.Index;
import org.apache.ignite.catalog.annotations.Table;
import org.apache.ignite.catalog.annotations.Zone;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.table.mapper.PojoMapper;
import org.junit.jupiter.api.Test;

@SuppressWarnings("LongLine")
class CreateTableFromAnnotationsTest {

    private static final Options quoteIdentifiers = Options.defaultOptions().prettyPrint(false).quoteIdentifiers(true);

    private static CreateTableFromAnnotationsImpl createTable() {
        return createTable(Options.defaultOptions().prettyPrint(false).quoteIdentifiers(false));
    }

    private static CreateTableFromAnnotationsImpl createTable(Options options) {
        return new CreateTableFromAnnotationsImpl(null, options).ifNotExists();
    }

    @Test
    void testMapperCompatibility() {
        var mapper = Mapper.of(Pojo.class);
        assertThat(mapper, instanceOf(PojoMapper.class));
        var m = (PojoMapper<Pojo>) mapper;

        assertThat(m.targetType(), is(Pojo.class));
        assertThat(m.fields(), containsInAnyOrder("id", "idStr", "firstName", "lastName", "str"));

        // mapper columns in uppercase
        assertThat(m.fieldForColumn("ID"), is("id"));
        assertThat(m.fieldForColumn("ID_STR"), is("idStr"));
        assertThat(m.fieldForColumn("F_NAME"), is("firstName"));
        assertThat(m.fieldForColumn("L_NAME"), is("lastName"));
        assertThat(m.fieldForColumn("STR"), is("str"));
    }

    @Test
    void testCreateFromKeyValueView() {
        // primitive/boxed key class is a primary key with default name 'id'
        var sql = createTable().keyValueView(Integer.class, PojoValue.class).getSql();
        assertThat(sql, containsString("CREATE TABLE IF NOT EXISTS pojo_value_test (id int, f_name varchar, l_name varchar, str varchar, PRIMARY KEY (id)) COLOCATE BY (id, id_str) WITH PRIMARY_ZONE='ZONE_TEST';"));

        // key class fields (annotated only) is a composite primary keys
        sql = createTable().keyValueView(PojoKey.class, PojoValue.class).getSql();
        assertThat(sql, containsString("CREATE ZONE IF NOT EXISTS zone_test ENGINE AIMEM WITH PARTITIONS=1, REPLICAS=3;"));
        assertThat(sql, containsString("CREATE TABLE IF NOT EXISTS pojo_value_test (id int, id_str varchar(20), f_name varchar, l_name varchar, str varchar, PRIMARY KEY (id, id_str)) COLOCATE BY (id, id_str) WITH PRIMARY_ZONE='ZONE_TEST';"));
        assertThat(sql, containsString("CREATE INDEX IF NOT EXISTS ix_pojo ON pojo_value_test (f_name, l_name desc);"));

        // quote identifiers
        sql = createTable(quoteIdentifiers).keyValueView(Integer.class, PojoValue.class).getSql();
        assertThat(sql, containsString("CREATE TABLE IF NOT EXISTS \"pojo_value_test\" (\"id\" int, \"f_name\" varchar, \"l_name\" varchar, \"str\" varchar, PRIMARY KEY (\"id\")) COLOCATE BY (\"id\", \"id_str\") WITH PRIMARY_ZONE='ZONE_TEST';"));

        sql = createTable(quoteIdentifiers).keyValueView(PojoKey.class, PojoValue.class).getSql();
        assertThat(sql, containsString("CREATE ZONE IF NOT EXISTS \"zone_test\" ENGINE AIMEM WITH PARTITIONS=1, REPLICAS=3;"));
        assertThat(sql, containsString("CREATE TABLE IF NOT EXISTS \"pojo_value_test\" (\"id\" int, \"id_str\" varchar(20), \"f_name\" varchar, \"l_name\" varchar, \"str\" varchar, PRIMARY KEY (\"id\", \"id_str\")) COLOCATE BY (\"id\", \"id_str\") WITH PRIMARY_ZONE='ZONE_TEST';"));
        assertThat(sql, containsString("CREATE INDEX IF NOT EXISTS \"ix_pojo\" ON \"pojo_value_test\" (\"f_name\", \"l_name\" desc);"));

    }

    @Test
    void testCreateFromRecordView() {
        var sql = createTable().recordView(Pojo.class).getSql();
        assertThat(sql, containsString("CREATE ZONE IF NOT EXISTS zone_test ENGINE AIMEM WITH PARTITIONS=1, REPLICAS=3;"));
        assertThat(sql, containsString("CREATE TABLE IF NOT EXISTS pojo_test (id int, id_str varchar(20), f_name varchar(20) not null default 'a', l_name varchar, str varchar, PRIMARY KEY (id, id_str)) COLOCATE BY (id, id_str) WITH PRIMARY_ZONE='ZONE_TEST';"));
        assertThat(sql, containsString("CREATE INDEX IF NOT EXISTS ix_pojo ON pojo_test (f_name, l_name desc);"));

        // quote identifiers
        sql = createTable(quoteIdentifiers).recordView(Pojo.class).getSql();
        assertThat(sql, containsString("CREATE ZONE IF NOT EXISTS \"zone_test\" ENGINE AIMEM WITH PARTITIONS=1, REPLICAS=3;"));
        assertThat(sql, containsString("CREATE TABLE IF NOT EXISTS \"pojo_test\" (\"id\" int, \"id_str\" varchar(20), \"f_name\" varchar(20) not null default 'a', \"l_name\" varchar, \"str\" varchar, PRIMARY KEY (\"id\", \"id_str\")) COLOCATE BY (\"id\", \"id_str\") WITH PRIMARY_ZONE='ZONE_TEST';"));
        assertThat(sql, containsString("CREATE INDEX IF NOT EXISTS \"ix_pojo\" ON \"pojo_test\" (\"f_name\", \"l_name\" desc);"));
    }

    @Test
    void testNameGeneration() {
        var sql = createTable().recordView(NameGeneration.class).getSql();
        assertThat(sql, containsString("CREATE TABLE IF NOT EXISTS NameGeneration ();"));
        assertThat(sql, containsString("CREATE INDEX IF NOT EXISTS ix_col1_col2 ON NameGeneration (col1, col2);"));
    }

    @Test
    void testPrimaryKey() {
        var sql = createTable().recordView(PkSort.class).getSql();
        assertThat(sql, containsString("CREATE TABLE IF NOT EXISTS PkSort (id int, PRIMARY KEY USING TREE (id desc))"));
    }

    @Zone(
            name = "zone_test",
            replicas = 3,
            partitions = 1,
            engine = ZoneEngine.AIMEM
    )
    static class ZoneTest {}

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

    @Table(
            name = "pojo_value_test",
            zone = ZoneTest.class,
            colocateBy = @ColocateBy(columns = {
                    @Col(name = "id"),
                    @Col(name = "id_str")
            }),
            indexes = {
                    @Index(name = "ix_pojo", columns = {
                            @Col(name = "f_name"),
                            @Col(name = "l_name", sort = SortOrder.DESC),
                    })
            }
    )
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

    @Table(
            name = "pojo_test",
            zone = ZoneTest.class,
            colocateBy = @ColocateBy(columns = {
                    @Col(name = "id"),
                    @Col(name = "id_str")
            }),
            indexes = {
                    @Index(name = "ix_pojo", columns = {
                            @Col(name = "f_name"),
                            @Col(name = "l_name", sort = SortOrder.DESC),
                    })
            }
    )
    static class Pojo {
        @Id
        Integer id;

        @Id
        @Column(name = "id_str", length = 20)
        String idStr;

        @Column(name = "f_name", columnDefinition = "varchar(20) not null default 'a'")
        String firstName;

        @Column(name = "l_name")
        String lastName;

        String str;

        Pojo() {}

        Pojo(Integer id, String idStr, String firstName, String lastName, String str) {
            this.id = id;
            this.idStr = idStr;
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
            Pojo pojo = (Pojo) o;
            return Objects.equals(id, pojo.id) && Objects.equals(idStr, pojo.idStr) && Objects.equals(firstName,
                    pojo.firstName) && Objects.equals(lastName, pojo.lastName) && Objects.equals(str, pojo.str);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, idStr, firstName, lastName, str);
        }
    }

    @Table(indexes = @Index(columns = { @Col(name = "col1"), @Col(name = "col2") }))
    static class NameGeneration {}

    @Table(primaryKeyType = IndexType.TREE)
    static class PkSort {
        @Id(sort = SortOrder.DESC)
        Integer id;
    }
}
