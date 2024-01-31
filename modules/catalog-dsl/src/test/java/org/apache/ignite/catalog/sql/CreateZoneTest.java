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

import static org.apache.ignite.catalog.ZoneEngine.ROCKSDB;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.apache.ignite.catalog.Options;
import org.junit.jupiter.api.Test;

class CreateZoneTest {

    private static final Options quoteIdentifiers = Options.defaultOptions().prettyPrint(false).quoteIdentifiers(true);
    private static CreateZoneImpl createZone() {
        return createZone(Options.defaultOptions().prettyPrint(false).quoteIdentifiers(false));
    }

    private static CreateZoneImpl createZone(Options options) {
        return new CreateZoneImpl(null, options);
    }

    @Test
    void testIfNotExists() {
        var sql = createZone().ifNotExists().name("zone1").getSql();
        assertThat(sql, is("CREATE ZONE IF NOT EXISTS zone1;"));

        sql = createZone(quoteIdentifiers).ifNotExists().name("zone1").getSql();
        assertThat(sql, is("CREATE ZONE IF NOT EXISTS \"zone1\";"));
    }

    @Test
    void testNames() {
        var sql = createZone().name("public", "zone1").getSql();
        assertThat(sql, is("CREATE ZONE public.zone1;"));

        // quote identifiers
        sql = createZone(quoteIdentifiers).name("public", "zone1").getSql();
        assertThat(sql, is("CREATE ZONE \"public\".\"zone1\";"));
    }

    @Test
    void testEngine() {
        var sql = createZone().name("zone1").engine(ROCKSDB).getSql();
        assertThat(sql, is("CREATE ZONE zone1 ENGINE ROCKSDB;"));

        sql = createZone(quoteIdentifiers).name("zone1").engine(ROCKSDB).getSql();
        assertThat(sql, is("CREATE ZONE \"zone1\" ENGINE ROCKSDB;"));
    }

    @Test
    void testWithOptions() {
        var sql = createZone().name("zone1").partitions(1).getSql();
        assertThat(sql, is("CREATE ZONE zone1 WITH PARTITIONS=1;"));

        sql = createZone().name("zone1").partitions(1).replicas(1).affinityFunction("f").getSql();
        assertThat(sql, is("CREATE ZONE zone1 WITH PARTITIONS=1, REPLICAS=1, AFFINITY_FUNCTION='f';"));

        // quote identifiers
        sql = createZone(quoteIdentifiers).name("zone1").partitions(1).getSql();
        assertThat(sql, is("CREATE ZONE \"zone1\" WITH PARTITIONS=1;"));

        sql = createZone(quoteIdentifiers).name("zone1").partitions(1).replicas(1).affinityFunction("f").getSql();
        assertThat(sql, is("CREATE ZONE \"zone1\" WITH PARTITIONS=1, REPLICAS=1, AFFINITY_FUNCTION='f';"));
    }

}