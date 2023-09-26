package org.apache.ignite.compute;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.compute.v2.ExecutorBuilder;
import org.apache.ignite.compute.v2.IgniteComputeV2;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
import org.junit.jupiter.api.Test;

public class IgniteComputeV2Test {

    private IgniteComputeV2 compute;

    private DeploymentUnit unit1;

    private DeploymentUnit unit2;

    private ClusterNode node1;

    private ClusterNode node2;

    @Test
    public void test() {
        CompletableFuture<String> asyncCall1 = compute.<String>executor(SimpleComputeJob.class.getName())
                .colocated(collocator -> collocator.toTuple(Tuple.create()))
                .async()
                .deploymentUnits(unit1)
                .call("arg1");

        CompletableFuture<String> asyncCall2 = compute.<String>executor(SimpleComputeJob.class.getName())
                .async()
                .colocated(collocator -> collocator.nodes(node1, node2))
                .deploymentUnits(unit1, unit2)
                .call(1, "arg");

        CustomClass syncCall1 = compute.<CustomClass>executor(CustomComputeJob.class)
                .colocated(collocator -> collocator.toKey(1, Mapper.of(Integer.class, "COLUMN_NAME")))
                .typeMapper(CustomMapper.class.getName())
                .call();
    }


    private static class CustomArg {

    }

    private static class CustomClass {

    }

    private static class CustomMapper {

    }

    private static class SimpleComputeJob implements ComputeJob<String> {

        @Override
        public String execute(JobExecutionContext context, Object... args) {
            return null;
        }
    }

    private static class CustomComputeJob implements ComputeJob<CustomClass> {

        @Override
        public CustomClass execute(JobExecutionContext context, Object... args) {
            return new CustomClass();
        }
    }
}
