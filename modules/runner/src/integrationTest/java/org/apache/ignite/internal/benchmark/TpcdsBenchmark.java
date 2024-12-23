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

package org.apache.ignite.internal.benchmark;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.sql.engine.util.TpcTable;
import org.apache.ignite.internal.sql.engine.util.tpcds.TpcdsHelper;
import org.apache.ignite.internal.sql.engine.util.tpcds.TpcdsTables;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Benchmark that runs sql queries from TPC-DS suite via embedded client.
 */
@State(Scope.Benchmark)
@Fork(1)
@Threads(1)
@Warmup(iterations = 10, time = 2)
@Measurement(iterations = 20, time = 2)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@SuppressWarnings({"WeakerAccess", "unused"})
public class TpcdsBenchmark extends AbstractTpcBenchmark {
    /*
        Minimal configuration of this benchmark requires specifying pathToDataset. Dataset is set of CSV
        files with name `{$tableName}.dat` per each table and character `|` as separator.

        By default, cluster's work directory will be created as a temporary folder. This implies,
        that all data generated by benchmark will be cleared automatically. However, this also implies
        that cluster will be recreated on EVERY RUN. To initialize cluster once and then reuse it state
        override `AbstractMultiNodeBenchmark.workDir()` method. Don't forget to clear that directory afterwards.
     */

    @Override
    TpcTable[] tablesToInit() {
        return TpcdsTables.values();
    }

    @Override
    Path pathToDataset() {
        throw new RuntimeException("Provide path to directory containing <table_name>.dat files");
    }

    @Param("1")
    private String queryId;

    private String queryString;

    /** Initializes a query string. */
    @Setup
    public void setUp() throws Throwable {
        try {
            queryString = TpcdsHelper.getQuery(queryId);
        } catch (Throwable e) {
            nodeTearDown();

            throw e;
        }
    }

    /** Benchmark that measures performance of queries from TPC-DS suite. */
    @Benchmark
    public void run(Blackhole bh) {
        try (var rs = sql.execute(null, queryString)) {
            while (rs.hasNext()) {
                bh.consume(rs.next());
            }
        }
    }

    /**
     * Benchmark's entry point.
     */
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(".*" + TpcdsBenchmark.class.getSimpleName() + ".*")
                .build();

        new Runner(opt).run();
    }
}
