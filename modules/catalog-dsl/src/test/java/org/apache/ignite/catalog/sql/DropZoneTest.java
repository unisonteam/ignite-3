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
import static org.hamcrest.Matchers.is;

import org.apache.ignite.catalog.Options;
import org.junit.jupiter.api.Test;

public class DropZoneTest {

    private static final Options quoteIdentifiers = Options.defaultOptions().prettyPrint(false).quoteIdentifiers(true);

    private static DropZoneImpl dropZone() {
        return dropZone(Options.defaultOptions().prettyPrint(false).quoteIdentifiers(false));
    }

    private static DropZoneImpl dropZone(Options options) {
        return new DropZoneImpl(null, options);
    }

    @Test
    void testDropZone() {
        var sql = dropZone().name("zone1").getSql();
        assertThat(sql, is("DROP ZONE zone1;"));

        sql = dropZone().ifExists().name("zone1").getSql();
        assertThat(sql, is("DROP ZONE IF EXISTS zone1;"));

        sql = dropZone(quoteIdentifiers).ifExists().name("zone1").getSql();
        assertThat(sql, is("DROP ZONE IF EXISTS \"zone1\";"));
    }
}
