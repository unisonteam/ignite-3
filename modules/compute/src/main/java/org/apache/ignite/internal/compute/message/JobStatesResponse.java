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

package org.apache.ignite.internal.compute.message;

import java.util.Collection;
import org.apache.ignite.compute.JobState;
import org.apache.ignite.internal.compute.ComputeMessageTypes;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.annotations.Marshallable;
import org.apache.ignite.internal.network.annotations.Transferable;
import org.jetbrains.annotations.Nullable;

/**
 * Remote job states response.
 */
@Transferable(ComputeMessageTypes.JOB_STATES_RESPONSE)
public interface JobStatesResponse extends NetworkMessage {
    @Nullable
    @Marshallable
    Collection<JobState> states();

    @Nullable
    @Marshallable
    Throwable throwable();
}