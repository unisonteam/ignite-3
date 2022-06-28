/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.cluster.management.rest;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;
import java.util.UUID;

/**
 * REST representation of {@link org.apache.ignite.internal.cluster.management.ClusterTag}.
 */
class ClusterTag {
    /** Auto-generated part. */
    private final UUID clusterId;

    /** Human-readable part. */
    private final String clusterName;

    @JsonCreator
    ClusterTag(@JsonProperty("clusterName") String clusterName, @JsonProperty("clusterId") UUID clusterId) {
        this.clusterName = clusterName;
        this.clusterId = clusterId;
    }

    @JsonProperty
    public UUID clusterId() {
        return clusterId;
    }

    @JsonProperty
    public String clusterName() {
        return clusterName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ClusterTag that = (ClusterTag) o;
        return clusterId.equals(that.clusterId) && clusterName.equals(that.clusterName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clusterId, clusterName);
    }

    @Override
    public String toString() {
        return "ClusterTag{"
                + "clusterId=" + clusterId
                + ", clusterName='" + clusterName + '\''
                + '}';
    }
}
