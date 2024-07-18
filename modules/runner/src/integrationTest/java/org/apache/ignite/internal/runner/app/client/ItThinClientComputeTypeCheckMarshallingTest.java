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

package org.apache.ignite.internal.runner.app.client;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.compute.ComputeException;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobStatus;
import org.apache.ignite.compute.JobTarget;
import org.apache.ignite.internal.runner.app.client.Jobs.ArgMarshalingJob;
import org.apache.ignite.internal.runner.app.client.Jobs.ResultMarshalingJob;
import org.apache.ignite.lang.ErrorGroups.Compute;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

/**
 * Test for exceptions that are thrown when marshalers are defined
 * in a wrong way or throw an exception.
 */
@SuppressWarnings("resource")
public class ItThinClientComputeTypeCheckMarshallingTest extends ItAbstractThinClientTest {
    @Test
    void argumentMarshalerDefinedOnlyInJob() {
        // Given.
        var node = server(0);

        // When submit job with custom marshaller that is defined in job but
        // client JobDescriptor does not declare the argument marshaler.
        var compute = computeClientOn(node);
        JobExecution<String> result = compute.submit(
                JobTarget.node(node(1)),
                JobDescriptor.builder(ArgMarshalingJob.class).build(),
                "Input"
        );

        assertStatusFailed(result);
        assertResultFailsWithErr(Compute.TYPE_CHECK_MARSHALING_ERR, result);
    }

    @Test
    void resultMarshalerDefinedOnlyInJob() {
        // Given.
        var node = server(0);

        // When submit job with custom marshaller that is defined in job but
        // client JobDescriptor does not declare the argument marshaler.
        var compute = computeClientOn(node);
        JobExecution<String> result = compute.submit(
                JobTarget.node(node(1)),
                JobDescriptor.builder(ResultMarshalingJob.class).build(),
                "Input"
        );

        assertStatusCompleted(result);
        assertThrows(ClassCastException.class, () -> {
            String str = getSafe(result.resultAsync());
        });
    }

    private static void assertResultFailsWithErr(int errCode, JobExecution<?> result) {
        var ex = assertThrows(CompletionException.class, () -> result.resultAsync().join());
        assertThat(ex.getCause(), instanceOf(ComputeException.class));
        assertThat(((ComputeException) ex.getCause()).code(), equalTo(errCode));
    }

    private static void assertStatusFailed(JobExecution<?> result) {
        var state = getSafe(result.stateAsync());
        assertThat(state, is(notNullValue()));
        assertThat(state.status(), equalTo(JobStatus.FAILED));
    }

    private static void assertStatusCompleted(JobExecution<?> result) {
        var state = getSafe(result.stateAsync());
        assertThat(state, is(notNullValue()));
        assertThat(state.status(), equalTo(JobStatus.COMPLETED));
    }

    private static <T> T getSafe(@Nullable CompletableFuture<T> fut) {
        assertThat(fut, is(notNullValue()));

        try {
            int waitSec = 5;
            return fut.get(waitSec, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    private IgniteCompute computeClientOn(Ignite node) {
        return IgniteClient.builder()
                .addresses(getClientAddresses(List.of(node)).toArray(new String[0]))
                .build()
                .compute();
    }

    private ClusterNode node(int idx) {
        return sortedNodes().get(idx);
    }

    private List<ClusterNode> sortedNodes() {
        return client().clusterNodes().stream()
                .sorted(Comparator.comparing(ClusterNode::name))
                .collect(Collectors.toList());
    }
}
