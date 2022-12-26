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

package org.apache.ignite.internal.e2e.core;

import java.time.Duration;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.e2e.Gobbler;

public class StepCompleter {
    public final IncompleteStep incompleteStep;
    private final History history;
    private final Gobbler errGobbler;
    private final Gobbler outGobbler;
    private final Executor executor;

    public StepCompleter(IncompleteStep incompleteStep, History history, Gobbler errGobbler, Gobbler outGobbler) {
        this.incompleteStep = incompleteStep;
        this.history = history;
        this.errGobbler = errGobbler;
        this.outGobbler = outGobbler;
        this.executor = Executors.newSingleThreadExecutor();
    }

    public void completeAsync() {
        AtomicLong withoutUpdatesTime = new AtomicLong(Duration.ofSeconds(3).toNanos());
        AtomicLong lastUpdateTime = new AtomicLong(System.nanoTime());
        executor.execute(() -> {
            lastUpdateTime.set(System.nanoTime());
            while (true) {
                String line = outGobbler.readLine();
                if (line != null) {
                    lastUpdateTime.set(System.nanoTime());
                    incompleteStep.outAdd(line);
                } else {
                    if (lastUpdateTime.get() + withoutUpdatesTime.get() < System.nanoTime()) {
                        history.completeStep();
                        return;
                    }
                }
            }
        });
    }
}
