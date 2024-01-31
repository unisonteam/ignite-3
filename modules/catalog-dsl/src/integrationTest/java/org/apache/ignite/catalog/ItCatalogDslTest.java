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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.catalog.annotations.Col;
import org.apache.ignite.catalog.annotations.ColocateBy;
import org.apache.ignite.catalog.annotations.Column;
import org.apache.ignite.catalog.annotations.Id;
import org.apache.ignite.catalog.annotations.Index;
import org.apache.ignite.catalog.annotations.Table;
import org.apache.ignite.catalog.annotations.Zone;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.RecordView;
import org.junit.jupiter.api.Test;

class ItCatalogDslTest extends ClusterPerTestIntegrationTest {

    @Test
    void testMapperKvView() {
        var key = 1;
        var expectedKey = new PojoKey(key, String.valueOf(key));
        var expectedValue = new PojoValue("fname", "lname", UUID.randomUUID().toString());

        // key boxed primitive
        node(0).catalog().createIfNotExists(Integer.class, PojoValue.class).execute();
        KeyValueView<Integer, PojoValue> kv = node(0).tables().table("pojo_value_test")
                .keyValueView(Integer.class, PojoValue.class);
        kv.put(null, key, expectedValue);
        var actual = kv.get(null, key);
        assertThat(actual, is(expectedValue));

        node(0).catalog().dropTableIfExists("pojo_value_test").execute();

        // key pojo
        node(0).catalog().createIfNotExists(PojoKey.class, PojoValue.class).execute();
        KeyValueView<PojoKey, PojoValue> kv2 = node(0).tables().table("pojo_value_test")
                .keyValueView(PojoKey.class, PojoValue.class);
        kv2.put(null, expectedKey, expectedValue);
        var actualValue = kv2.get(null, expectedKey);
        assertThat(actualValue, is(expectedValue));

        node(0).catalog().dropTableIfExists("pojo_value_test").execute();
    }

    @Test
    void testMapperRecordView() {
        var expected = new Pojo(1, "1", "fname", "lname", UUID.randomUUID().toString());
        node(0).catalog().createIfNotExists(Pojo.class).execute();

        RecordView<Pojo> rec = node(0).tables().table("pojo_test").recordView(Pojo.class);
        var ins = rec.insert(null, expected);
        assertTrue(ins);

        var actual = rec.get(null, expected);
        assertThat(actual, is(expected))
        ;
        node(0).catalog().dropTableIfExists("pojo_test").execute();
    }

    @Test
    void testSqlInsertSelect() {
        node(0).catalog().createIfNotExists(Pojo.class).execute();

        try (var session = node(0).sql().createSession()) {
            session.execute(null, "insert into pojo_test (id, id_str, f_name, l_name, str) values (1, '1', 'f', 'l', 's')");

            try (var rs = session.execute(null, "select * from pojo_test")) {
                while (rs.hasNext()) {
                    var row = rs.next();
                    assertThat(row.value("id"), is(1));
                    assertThat(row.value("id_str"), is("1"));
                    assertThat(row.value("f_name"), is("f"));
                    assertThat(row.value("l_name"), is("l"));
                    assertThat(row.value("str"), is("s"));
                }
            }
        }

        node(0).catalog().dropTableIfExists("pojo_test").execute();
    }

    @Zone(
            name = "zone_test"
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
            colocateBy = @ColocateBy(columns = @Col(name = "id")),
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

        PojoValue() {}

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
            colocateBy = @ColocateBy(columns = @Col(name = "id")),
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

}
