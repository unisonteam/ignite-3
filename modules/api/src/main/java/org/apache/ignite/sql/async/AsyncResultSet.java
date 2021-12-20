/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.sql.async;

import java.util.concurrent.CompletionStage;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.sql.SqlRow;

/**
 * Asynchronous result set.
 */
public interface AsyncResultSet {
    /**
     * Returns metadata for the results.
     *
     * @return ResultSet metadata.
     */
    ResultSetMetadata metadata();

    /**
     * Returns whether the result set contains rows (SELECT query result), or not (for query of DML, DDL or other kind).
     *
     * @return {@code True} if result set contains rows, {@code false} otherwise.
     */
    boolean hasRowSet();

    /**
     * Returns number of row affected by DML query.
     *
     * @return Number of rows.
     */
    int updateCount();

    /**
     * Returns result for the conditional query.
     *
     * @return {@code True} if conditional query applied, {@code false} otherwise.
     */
    boolean wasApplied();

    /**
     * Returns the current page content.
     *
     * @return Iterable over rows.
     */
    Iterable<SqlRow> currentPage();

    /**
     * Fetch the next page of results asynchronously.
     *
     * @return Operation future.
     */
    CompletionStage<? extends AsyncResultSet> fetchNextPageAsync();

    /**
     * Returns whether there are more pages of results.
     *
     * @return {@code True} if there are more pages with results, {@code false} otherwise.
     */
    boolean hasMorePages();
}
