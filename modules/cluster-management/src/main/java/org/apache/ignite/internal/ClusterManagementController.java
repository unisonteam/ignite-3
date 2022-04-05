package org.apache.ignite.internal;

import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.util.concurrent.ExecutionException;
import org.apache.ignite.internal.cluster.management.ClusterInitializer;
import org.apache.ignite.internal.cluster.management.rest.InitCommand;
import org.apache.ignite.lang.IgniteLogger;

//todo
@Controller("/management/v1/configuration/init")
@ApiResponse(responseCode = "400", description = "Incorrect body")
@ApiResponse(responseCode = "500", description = "Internal error")
@Tag(name = "clusterManagement")
public class ClusterManagementController {
    private static final IgniteLogger log = IgniteLogger.forClass(ClusterManagementController.class);

    private final ClusterInitializer clusterInitializer;

    public ClusterManagementController(ClusterInitializer clusterInitializer) {
        this.clusterInitializer = clusterInitializer;
    }

    @Post
    @Operation(operationId = "init")
    @ApiResponse(responseCode = "200", description = "Cluster initialized")
    public void init(InitCommand initCommand) throws ExecutionException, InterruptedException {
        if (log.isInfoEnabled()) {
            log.info(
                    "Received init command:\n\tMeta Storage nodes: {}\n\tCMG nodes: {}",
                    initCommand.metaStorageNodes(),
                    initCommand.cmgNodes()
            );
        }

        clusterInitializer.initCluster(initCommand.metaStorageNodes(), initCommand.cmgNodes()).get();
    }
}
