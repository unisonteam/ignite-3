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

package org.apache.ignite.internal.compute;

import static org.apache.ignite.compute.JobStatus.EXECUTING;
import static org.apache.ignite.compute.JobStatus.QUEUED;
import static org.apache.ignite.internal.IgniteExceptionTestUtils.assertPublicCheckedException;
import static org.apache.ignite.internal.IgniteExceptionTestUtils.assertPublicException;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.will;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.JobStateMatcher.jobStateWithStatus;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.compute.BroadcastExecution;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.compute.JobExecutionOptions;
import org.apache.ignite.compute.JobTarget;
import org.apache.ignite.deployment.DeploymentUnit;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.lang.IgniteCheckedException;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Table;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Integration tests for Compute functionality in embedded Ignite mode.
 */
@SuppressWarnings({"NewClassNamingConvention"})
class ItComputeTestEmbedded extends ItComputeBaseTest {

    @Override
    protected List<DeploymentUnit> units() {
        return List.of();
    }

    @Test
    void changeJobPriorityLocally() {
        JobTarget jobTarget = JobTarget.node(clusterNode(0));

        CountDownLatch countDownLatch = new CountDownLatch(1);
        JobDescriptor<CountDownLatch, String> job = JobDescriptor.builder(WaitLatchJob.class).units(units()).build();

        // Start 1 task in executor with 1 thread
        JobExecution<String> execution1 = submit(jobTarget, job, countDownLatch);
        await().until(execution1::stateAsync, willBe(jobStateWithStatus(EXECUTING)));

        // Start one more task
        JobExecution<String> execution2 = submit(jobTarget, job, new CountDownLatch(1));
        await().until(execution2::stateAsync, willBe(jobStateWithStatus(QUEUED)));

        // Start third task
        JobExecution<String> execution3 = submit(jobTarget, job, countDownLatch);
        await().until(execution3::stateAsync, willBe(jobStateWithStatus(QUEUED)));

        // Task 2 and 3 are not completed, in queued state
        assertThat(execution2.resultAsync().isDone(), is(false));
        assertThat(execution3.resultAsync().isDone(), is(false));

        // Change priority of task 3, so it should be executed before task 2
        assertThat(execution3.changePriorityAsync(2), willBe(true));

        // Run 1 and 3 task
        countDownLatch.countDown();

        // Tasks 1 and 3 completed successfully
        assertThat(execution1.resultAsync(), willCompleteSuccessfully());
        assertThat(execution3.resultAsync(), willCompleteSuccessfully());

        // Task 2 is not completed
        assertThat(execution2.resultAsync().isDone(), is(false));

        // Finish task 2
        assertThat(execution2.cancelAsync(), willBe(true));
    }

    @Test
    void executesJobLocallyWithOptions() {
        JobTarget jobTarget = JobTarget.node(clusterNode(0));

        CountDownLatch countDownLatch = new CountDownLatch(1);
        JobDescriptor<CountDownLatch, String> job = JobDescriptor.builder(WaitLatchJob.class).units(units()).build();

        // Start 1 task in executor with 1 thread
        JobExecution<String> execution1 = submit(jobTarget, job, countDownLatch);

        await().until(execution1::stateAsync, willBe(jobStateWithStatus(EXECUTING)));

        // Start one more task
        JobExecution<String> execution2 = submit(jobTarget, job, new CountDownLatch(1));
        await().until(execution2::stateAsync, willBe(jobStateWithStatus(QUEUED)));

        // Start third task it should be before task2 in the queue due to higher priority in options
        JobExecutionOptions options = JobExecutionOptions.builder().priority(1).maxRetries(2).build();
        JobExecution<String> execution3 = submit(
                jobTarget,
                JobDescriptor.builder(WaitLatchThrowExceptionOnFirstExecutionJob.class)
                        .units(units())
                        .options(options)
                        .build(),
                countDownLatch
        );
        await().until(execution3::stateAsync, willBe(jobStateWithStatus(QUEUED)));

        // Task 1 and 2 are not competed, in queue state
        assertThat(execution2.resultAsync().isDone(), is(false));
        assertThat(execution3.resultAsync().isDone(), is(false));

        // Reset counter
        WaitLatchThrowExceptionOnFirstExecutionJob.counter.set(0);

        // Run 1 and 3 task
        countDownLatch.countDown();

        // Tasks 1 and 3 completed successfully
        assertThat(execution1.resultAsync(), willCompleteSuccessfully());
        assertThat(execution3.resultAsync(), willCompleteSuccessfully());

        // Task 3 should be executed 2 times
        assertEquals(2, WaitLatchThrowExceptionOnFirstExecutionJob.counter.get());

        // Task 2 is not completed
        assertThat(execution2.resultAsync().isDone(), is(false));

        // Cancel task2
        assertThat(execution2.cancelAsync(), willBe(true));
    }

    @Test
    void shouldNotConvertIgniteException() {
        Ignite entryNode = node(0);

        IgniteException exception = new IgniteException(INTERNAL_ERR, "Test exception");

        IgniteException ex = assertThrows(IgniteException.class, () -> entryNode.compute().execute(
                JobTarget.node(clusterNode(entryNode)),
                JobDescriptor.builder(CustomFailingJob.class).units(units()).build(),
                exception));

        assertPublicException(ex, exception.code(), exception.getMessage());
    }

    @Test
    void shouldNotConvertIgniteCheckedException() {
        Ignite entryNode = node(0);

        IgniteCheckedException exception = new IgniteCheckedException(INTERNAL_ERR, "Test exception");

        IgniteCheckedException ex = assertThrows(IgniteCheckedException.class, () -> entryNode.compute().execute(
                JobTarget.node(clusterNode(entryNode)),
                JobDescriptor.builder(CustomFailingJob.class).units(units()).build(),
                exception));

        assertPublicCheckedException(ex, exception.code(), exception.getMessage());
    }

    private static Stream<Arguments> privateExceptions() {
        return Stream.of(
                Arguments.of(new IgniteInternalException(INTERNAL_ERR, "Test exception")),
                Arguments.of(new IgniteInternalCheckedException(INTERNAL_ERR, "Test exception")),
                Arguments.of(new RuntimeException("Test exception")),
                Arguments.of(new Exception("Test exception"))
        );
    }

    @ParameterizedTest
    @MethodSource("privateExceptions")
    void shouldConvertToComputeException(Throwable throwable) {
        Ignite entryNode = node(0);

        IgniteException ex = assertThrows(IgniteException.class, () -> entryNode.compute().execute(
                JobTarget.node(clusterNode(entryNode)),
                JobDescriptor.builder(CustomFailingJob.class).units(units()).build(),
                throwable));

        assertComputeException(ex, throwable);
    }

    @ParameterizedTest
    @MethodSource("targetNodeIndexes")
    void executesSyncKvGetPutFromJob(int targetNodeIndex) {
        createTestTableWithOneRow();

        int entryNodeIndex = 0;

        Ignite entryNode = node(entryNodeIndex);
        Ignite targetNode = node(targetNodeIndex);

        assertDoesNotThrow(() -> entryNode.compute().execute(
                JobTarget.node(clusterNode(targetNode)),
                JobDescriptor.builder(PerformSyncKvGetPutJob.class).build(), null));
    }

    @Test
    void executesNullReturningJobViaSyncBroadcast() {
        Ignite entryNode = node(0);

        Collection<Object> results = entryNode.compute()
                .executeBroadcast(new HashSet<>(entryNode.clusterNodes()), JobDescriptor.builder(NullReturningJob.class).build(), null);

        assertThat(results, everyItem(nullValue()));
    }

    @Test
    void executesNullReturningJobViaAsyncBroadcast() {
        Ignite entryNode = node(0);

        CompletableFuture<Collection<Object>> resultsFuture = entryNode.compute().executeBroadcastAsync(
                new HashSet<>(entryNode.clusterNodes()),
                JobDescriptor.builder(NullReturningJob.class).build(),
                null
        );
        assertThat(resultsFuture, will(everyItem(nullValue())));
    }

    @Test
    void executesNullReturningJobViaSubmitBroadcast() {
        Ignite entryNode = node(0);

        CompletableFuture<BroadcastExecution<Object>> executionFut = entryNode.compute().submitAsync(
                new HashSet<>(entryNode.clusterNodes()),
                JobDescriptor.builder(NullReturningJob.class).build(),
                null
        );
        assertThat(executionFut, willCompleteSuccessfully());
        BroadcastExecution<Object> execution = executionFut.join();

        assertThat(execution.resultsAsync(), will(everyItem(nullValue())));
    }

    private Stream<Arguments> targetNodeIndexes() {
        return IntStream.range(0, initialNodes()).mapToObj(Arguments::of);
    }

    private static class CustomFailingJob implements ComputeJob<Throwable, String> {
        @Override
        public CompletableFuture<String> executeAsync(JobExecutionContext context, Throwable th) {
            throw ExceptionUtils.sneakyThrow(th);
        }
    }

    private static class WaitLatchJob implements ComputeJob<CountDownLatch, String> {
        @Override
        public CompletableFuture<String> executeAsync(JobExecutionContext context, CountDownLatch latch) {
            try {
                latch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return null;
        }
    }

    private static class WaitLatchThrowExceptionOnFirstExecutionJob implements ComputeJob<CountDownLatch, String> {
        static final AtomicInteger counter = new AtomicInteger(0);

        @Override
        public CompletableFuture<String> executeAsync(JobExecutionContext context, CountDownLatch latch) {
            try {
                latch.await();
                if (counter.incrementAndGet() == 1) {
                    throw new RuntimeException();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return null;
        }
    }

    private static class PerformSyncKvGetPutJob implements ComputeJob<Void, Void> {
        @Override
        public CompletableFuture<Void> executeAsync(JobExecutionContext context, Void input) {
            Table table = context.ignite().tables().table("test");
            KeyValueView<Integer, Integer> view = table.keyValueView(Integer.class, Integer.class);

            view.get(null, 1);
            view.put(null, 1, 1);

            return null;
        }
    }

    private static class NullReturningJob implements ComputeJob<Object, Object> {
        @Override
        public CompletableFuture<Object> executeAsync(JobExecutionContext context, Object input) {
            return null;
        }
    }
}
