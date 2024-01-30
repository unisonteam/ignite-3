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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

import java.util.Objects;
import org.apache.ignite.ddl.Options;
import org.apache.ignite.ddl.annotations.Column;
import org.apache.ignite.ddl.annotations.Id;
import org.apache.ignite.ddl.sql.CreateTableFromClassTest.Pojo;
import org.junit.jupiter.api.Test;

@SuppressWarnings("LongLine")
public class CreateTableFromClassExtendedTest {

    private static final Options quoteIdentifiers = Options.defaultOptions().prettyPrint(false).quoteIdentifiers(true);

    private static CreateTableFromClassExtendedImpl createTable(String name) {
        return createTable(Options.defaultOptions().prettyPrint(false).quoteIdentifiers(false), name);
    }

    private static CreateTableFromClassExtendedImpl createTable(Options options, String name) {
        return new CreateTableFromClassExtendedImpl(null, options).name(name);
    }

    @Test
    void testCreateFromKeyValueView() {
        // primitive/boxed key class is a primary key with default name 'id'
        var sql = createTable("pojo_value_test")
                .ifNotExists()
                .colocateBy("id", "id_str")
                .zone("zone_test")
                .keyValueView(Integer.class, PojoValue.class)
                .getSql();
        assertThat(sql, containsString("CREATE TABLE IF NOT EXISTS pojo_value_test (id int, f_name varchar, l_name varchar, str varchar, PRIMARY KEY (id)) COLOCATE BY (id, id_str) WITH PRIMARY_ZONE='ZONE_TEST';"));

        // key class fields (annotated only) is a composite primary keys
        sql = createTable("pojo_value_test")
                .ifNotExists()
                .colocateBy("id", "id_str")
                .zone("zone_test")
                .keyValueView(PojoKey.class, PojoValue.class)
                .getSql();
        assertThat(sql, containsString("CREATE TABLE IF NOT EXISTS pojo_value_test (id int, id_str varchar(20), f_name varchar, l_name varchar, str varchar, PRIMARY KEY (id, id_str)) COLOCATE BY (id, id_str) WITH PRIMARY_ZONE='ZONE_TEST';"));
    }

    @Test
    void testCreateFromRecordView() {
        var sql = createTable("pojo_test")
                .ifNotExists()
                .colocateBy("id", "id_str")
                .zone("zone_test")
                .recordView(Pojo.class)
                .getSql();
        assertThat(sql, containsString("CREATE TABLE IF NOT EXISTS pojo_test (id int, id_str varchar(20), f_name varchar(20) not null default 'a', l_name varchar, str varchar, PRIMARY KEY (id, id_str)) COLOCATE BY (id, id_str) WITH PRIMARY_ZONE='ZONE_TEST';"));

        // quote identifiers
        sql = createTable(quoteIdentifiers, "pojo_test")
                .ifNotExists()
                .colocateBy("id", "id_str")
                .zone("zone_test")
                .recordView(Pojo.class)
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
