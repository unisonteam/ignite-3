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

package org.apache.ignite.internal.rest.sql;

import static io.micronaut.http.HttpRequest.DELETE;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Awaitility.waitAtMost;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import io.micronaut.core.type.Argument;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.rest.api.sql.SqlQueryInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for {@link SqlQueryController}.
 */
@MicronautTest
public class ItSqlQueryControllerTest extends ClusterPerClassIntegrationTest {
    private static final String SQL_QUERY_URL = "/management/v1/sql/";

    @Inject
    @Client("http://localhost:10300" + SQL_QUERY_URL)
    HttpClient client;

    @AfterEach
    void tearDown() {
        sql("DROP TABLE large_table");
    }

    @Test
    void shouldReturnAllSqlQueries() {
        // Create table
        sql("CREATE TABLE large_table (id int primary key, value1 DOUBLE, value2 DOUBLE)");

        // Run long running query async
        String sql = "INSERT INTO large_table (id, value1, value2) SELECT x, RAND() * 100, RAND() * 100 FROM TABLE(SYSTEM_RANGE(1, 100));";
        CompletableFuture.runAsync(() ->
                sql(sql)
        );

        // Check count
        await().untilAsserted(() -> {
            Map<UUID, SqlQueryInfo> queries = getSqlQueries(client);

            assertThat(queries, aMapWithSize(1));
            SqlQueryInfo queryInfo = queries.entrySet().iterator().next().getValue();

            assertThat(queryInfo.sql(), is(sql));
            assertThat(queryInfo.schema(), is("PUBLIC"));
            assertThat(queryInfo.type(), is("DML"));
        });
    }

    @Test
    void shouldReturnSingleQuery() {
        // Create table
        sql("CREATE TABLE large_table (id int primary key, value1 DOUBLE, value2 DOUBLE)");

        // Run long running query async
        String sql = "INSERT INTO large_table (id, value1, value2) SELECT x, RAND() * 100, RAND() * 100 FROM TABLE(SYSTEM_RANGE(1, 10));";
        CompletableFuture.runAsync(() ->
                sql(sql)
        );

        Map<UUID, SqlQueryInfo> queries = getSqlQueries(client);

        assertThat(queries, aMapWithSize(1));
        Map.Entry<UUID, SqlQueryInfo> sqlQueryInfoEntry = queries.entrySet().iterator().next();

        SqlQueryInfo query = getSqlQuery(client, sqlQueryInfoEntry.getKey());
        assertThat(query, equalTo(sqlQueryInfoEntry.getValue()));
    }

    @Test
    void shouldCancelSqlQuery() {
        // Create table
        sql("CREATE TABLE large_table (id int primary key, value1 DOUBLE, value2 DOUBLE)");

        // Run long running query async
        String sql = "INSERT INTO large_table (id, value1, value2) SELECT x, RAND() * 100, RAND() * 100 FROM TABLE(SYSTEM_RANGE(1, 1000));";
        CompletableFuture.runAsync(() ->
                sql(sql)
        );

        // Check count
        await().untilAsserted(() -> {
            Map<UUID, SqlQueryInfo> queries = getSqlQueries(client);

            assertThat(queries, aMapWithSize(1));
            SqlQueryInfo queryInfo = queries.entrySet().iterator().next().getValue();

            cancelSqlQuery(client, queryInfo.id());

            waitAtMost(Duration.ofSeconds(10)).until(() -> getSqlQuery(client, queryInfo.id()), is(null));
        });
    }

    private static Map<UUID, SqlQueryInfo> getSqlQueries(HttpClient client) {
        List<SqlQueryInfo> sqlQueries = client.toBlocking()
                .retrieve(HttpRequest.GET("/queries"), Argument.listOf(SqlQueryInfo.class));

        return sqlQueries.stream().collect(Collectors.toMap(SqlQueryInfo::id, s -> s));
    }

    private static SqlQueryInfo getSqlQuery(HttpClient client, UUID queryId) {
        return client.toBlocking().retrieve(HttpRequest.GET("/queries/" + queryId), SqlQueryInfo.class);
    }

    private static void cancelSqlQuery(HttpClient client, UUID queryId) {
        client.toBlocking().exchange(DELETE("/queries/" + queryId));
    }
}
