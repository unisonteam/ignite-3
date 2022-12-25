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

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class History {
    private final List<CompleteStep> completedSteps;
    private final ReentrantLock lock;
    private final Condition notRunning;
    private final Condition running;
    private IncompleteStep runningStep;

    private History(List<CompleteStep> completedSteps, IncompleteStep runningStep) {
        this.completedSteps = completedSteps;
        this.runningStep = runningStep;
        this.lock = new ReentrantLock();
        this.notRunning = lock.newCondition();
        this.running = lock.newCondition();
    }

    public static History empty() {
        return new History(new CopyOnWriteArrayList<>(), null);
    }

    public void run(IncompleteStep incompleteStep) {
        lock.lock();
        try {
            while (runningStep != null) {
                notRunning.await();
            }
            runningStep = incompleteStep;
            running.signalAll();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }
    }

    public void completeStep() {
        lock.lock();
        try {
            while (runningStep == null) {
                running.await();
            }
            completedSteps.add(runningStep.complete());
            runningStep = null;
            notRunning.signalAll();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }
    }

    public void waitStepCompleted() {
        lock.lock();
        try {
            while (runningStep != null) {
                notRunning.await();
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }
    }

    public CompleteStep findLastStep() {
        return completedSteps.get(completedSteps.size() - 1); // todo
    }
}
