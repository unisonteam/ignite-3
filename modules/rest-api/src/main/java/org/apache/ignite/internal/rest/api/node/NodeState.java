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

package org.apache.ignite.internal.rest.api.node;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.media.Schema.RequiredMode;

/**
 * Node state that is returned by REST.
 */
@Schema(description = "Node state.")
public class NodeState {
    @Schema(description = "Unique node name.", requiredMode = RequiredMode.REQUIRED)
    private final String name;

    @Schema(description = "Node status.", requiredMode = RequiredMode.REQUIRED)
    private final State state;

    @Schema(description = "Node jdbc port.", requiredMode = RequiredMode.REQUIRED)
    private final int jdbcPort;

    /**
     * Construct NodeState DTO.
     */
    @JsonCreator
    public NodeState(@JsonProperty("name") String name,
            @JsonProperty("state") State state,
            @JsonProperty("jdbcPort") int jdbcPort) {
        this.name = name;
        this.state = state;
        this.jdbcPort = jdbcPort;
    }

    @JsonGetter("name")
    public String name() {
        return name;
    }

    @JsonGetter("state")
    public State state() {
        return state;
    }

    @JsonGetter("jdbcPort")
    public int jdbcPort() {
        return jdbcPort;
    }
}
