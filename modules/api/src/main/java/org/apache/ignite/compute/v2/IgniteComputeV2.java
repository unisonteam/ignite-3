package org.apache.ignite.compute.v2;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.compute.v2.chooser.NodeChooser;

public interface IgniteComputeV2 extends ComputeManagement {

    <T> T execute(JobConfiguration job, NodeChooser nodeChooser);

    <T> CompletableFuture<T> executeAsync(JobConfiguration job, NodeChooser nodeChooser);
}
