package org.apache.ignite.compute.v2;

public interface IgniteComputeV2 extends ComputeManagement {

    <T> ExecutorBuilder<T> executor();
}
