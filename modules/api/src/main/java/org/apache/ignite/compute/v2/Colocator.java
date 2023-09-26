package org.apache.ignite.compute.v2;

import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;

public interface Colocator {
    TableCollocator table(String table);

    Colocator nodes(ClusterNode... nodes);
}
