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

package org.apache.ignite.catalog;

public class ZoneDefinition {
    private final String zoneName;
    private final boolean ifNotExists;
    private final Integer partitions;
    private final Integer replicas;
    private final Integer dataNodesAutoAdjust;
    private final Integer dataNodesAutoAdjustScaleUp;
    private final Integer dataNodesAutoAdjustScaleDown;
    private final String filter;
    private final ZoneEngine engine;
    private final String dataregion;

    private ZoneDefinition(
            String zoneName,
            boolean ifNotExists,
            Integer partitions,
            Integer replicas,
            Integer dataNodesAutoAdjust,
            Integer dataNodesAutoAdjustScaleUp,
            Integer dataNodesAutoAdjustScaleDown,
            String filter,
            ZoneEngine engine,
            String dataregion) {
        this.zoneName = zoneName;
        this.ifNotExists = ifNotExists;
        this.partitions = partitions;
        this.replicas = replicas;
        this.dataNodesAutoAdjust = dataNodesAutoAdjust;
        this.dataNodesAutoAdjustScaleUp = dataNodesAutoAdjustScaleUp;
        this.dataNodesAutoAdjustScaleDown = dataNodesAutoAdjustScaleDown;
        this.filter = filter;
        this.engine = engine;
        this.dataregion = dataregion;
    }

    public static Builder builder(String zoneName) {
        return new Builder().zoneName(zoneName);
    }

    public String getZoneName() {
        return zoneName;
    }

    public boolean ifNotExists() {
        return ifNotExists;
    }

    public Integer getPartitions() {
        return partitions;
    }

    public Integer getReplicas() {
        return replicas;
    }

    public Integer getDataNodesAutoAdjust() {
        return dataNodesAutoAdjust;
    }

    public Integer getDataNodesAutoAdjustScaleUp() {
        return dataNodesAutoAdjustScaleUp;
    }

    public Integer getDataNodesAutoAdjustScaleDown() {
        return dataNodesAutoAdjustScaleDown;
    }

    public String getFilter() {
        return filter;
    }

    public ZoneEngine getEngine() {
        return engine;
    }

    public String getDataregion() {
        return dataregion;
    }

    public Builder toBuilder() {
        return new Builder(this);
    }

    public static class Builder {

        private String zoneName;
        private boolean ifNotExists;
        private Integer partitions;
        private Integer replicas;
        private Integer dataNodesAutoAdjust;
        private Integer dataNodesAutoAdjustScaleUp;
        private Integer dataNodesAutoAdjustScaleDown;
        private String filter;
        private ZoneEngine engine = ZoneEngine.DEFAULT;
        private String dataregion;


        private Builder() {}

        private Builder(ZoneDefinition definition) {
            zoneName = definition.zoneName;
            ifNotExists = definition.ifNotExists;
            partitions = definition.partitions;
            replicas = definition.replicas;
            dataNodesAutoAdjust = definition.dataNodesAutoAdjust;
            dataNodesAutoAdjustScaleUp = definition.dataNodesAutoAdjustScaleUp;
            dataNodesAutoAdjustScaleDown = definition.dataNodesAutoAdjustScaleDown;
            filter = definition.filter;
            engine = definition.engine;
            dataregion = definition.dataregion;
        }

        Builder zoneName(String zoneName) {
            this.zoneName = zoneName;
            return this;
        }

        public Builder ifNotExists() {
            this.ifNotExists = true;
            return this;
        }

        public Builder partitions(Integer partitions) {
            this.partitions = partitions;
            return this;
        }

        public Builder replicas(Integer replicas) {
            this.replicas = replicas;
            return this;
        }

        public Builder dataNodesAutoAdjust(Integer adjust) {
            this.dataNodesAutoAdjust = adjust;
            return this;
        }

        public Builder dataNodesAutoAdjustScaleUp(Integer adjust) {
            this.dataNodesAutoAdjustScaleUp = adjust;
            return this;
        }

        public Builder dataNodesAutoAdjustScaleDown(Integer adjust) {
            this.dataNodesAutoAdjustScaleDown = adjust;
            return this;
        }

        public Builder filter(String filter) {
            this.filter = filter;
            return this;
        }

        public Builder engine(ZoneEngine engine) {
            this.engine = engine;
            return this;
        }

        public Builder dataregion(String dataregion) {
            this.dataregion = dataregion;
            return this;
        }

        public ZoneDefinition build() {
            return new ZoneDefinition(
                    zoneName,
                    ifNotExists,
                    partitions,
                    replicas,
                    dataNodesAutoAdjust,
                    dataNodesAutoAdjustScaleUp,
                    dataNodesAutoAdjustScaleDown,
                    filter,
                    engine,
                    dataregion);
        }
    }
}
