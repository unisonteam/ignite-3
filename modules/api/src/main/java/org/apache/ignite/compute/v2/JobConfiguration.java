package org.apache.ignite.compute.v2;

import java.util.Collection;
import org.apache.ignite.compute.DeploymentUnit;

public interface JobConfiguration {
    String jobClassName();

    Object[] args();

    Collection<DeploymentUnit> deploymentUnits();

    long priority();

    int retryOnFail();
}
