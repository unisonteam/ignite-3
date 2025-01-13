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

namespace Apache.Ignite.Compute;

using System.Collections.Generic;
using Internal.Common;
using Network;

/// <summary>
/// Compute broadcast job target.
/// </summary>
public static class BroadcastJobTarget
{
    /// <summary>
    /// Creates a broadcast job target for all specified nodes.
    /// </summary>
    /// <param name="nodes">Nodes to run the job on.</param>
    /// <returns>Job target.</returns>
    public static IBroadcastJobTarget<IEnumerable<IClusterNode>> AllNodes(IEnumerable<IClusterNode> nodes)
    {
        IgniteArgumentCheck.NotNull(nodes);

        return new AllNodesTarget(nodes);
    }

    /// <summary>
    /// Creates a broadcast job target for all specified nodes.
    /// </summary>
    /// <param name="nodes">Nodes to run the job on.</param>
    /// <returns>Job target.</returns>
    public static IBroadcastJobTarget<IEnumerable<IClusterNode>> AllNodes(params IClusterNode[] nodes)
    {
        IgniteArgumentCheck.NotNull(nodes);

        return new AllNodesTarget(nodes);
    }

    /// <summary>
    /// All nodes broadcast job target.
    /// </summary>
    /// <param name="Data">Nodes.</param>
    internal record AllNodesTarget(IEnumerable<IClusterNode> Data) : IBroadcastJobTarget<IEnumerable<IClusterNode>>;
}
