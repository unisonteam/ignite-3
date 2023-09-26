package org.apache.ignite.compute.v2;

import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;

public interface TableCollocator {
    Colocator toTuple(Tuple tuple);

    <T> Colocator toKey(T key, Mapper<T> mapper);
}
