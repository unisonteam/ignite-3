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

package org.apache.ignite.internal.storage.rocksdb.index;

import static java.util.Comparator.comparing;
import static org.apache.ignite.internal.schema.SchemaTestUtils.generateRandomValue;
import static org.apache.ignite.internal.storage.rocksdb.index.ComparatorUtils.comparingNull;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.randomBytes;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.Objects;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.apache.ignite.internal.schema.SchemaTestUtils;
import org.apache.ignite.internal.storage.index.IndexRow;
import org.apache.ignite.internal.storage.index.IndexRowPrefix;
import org.apache.ignite.internal.storage.index.SortedIndexDescriptor;
import org.apache.ignite.internal.storage.index.SortedIndexDescriptor.ColumnDescriptor;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.jetbrains.annotations.NotNull;

/**
 * Convenience wrapper over an Index row.
 */
class IndexRowWrapper implements Comparable<IndexRowWrapper> {
    /**
     * Values used to create the Index row.
     */
    private final Object[] columns;

    private final IndexRow row;

    private final SortedIndexDescriptor descriptor;

    IndexRowWrapper(SortedIndexStorage storage, IndexRow row, Object[] columns) {
        this.descriptor = storage.indexDescriptor();
        this.row = row;
        this.columns = columns;
    }

    /**
     * Creates an Entry with a random key that satisfies the given schema and a random value.
     */
    static IndexRowWrapper randomRow(SortedIndexStorage indexStorage) {
        var random = new Random();

        Object[] columns = indexStorage.indexDescriptor().indexRowColumns().stream()
                .map(ColumnDescriptor::column)
                .map(column -> generateRandomValue(random, column.type()))
                .toArray();

        var primaryKey = new ByteArraySearchRow(randomBytes(random, 25));

        IndexRow row = indexStorage.indexRowFactory().createIndexRow(columns, primaryKey);

        return new IndexRowWrapper(indexStorage, row, columns);
    }

    /**
     * Creates an Index Key prefix of the given length.
     */
    IndexRowPrefix prefix(int length) {
        return () -> Arrays.copyOf(columns, length);
    }

    IndexRow row() {
        return row;
    }

    Object[] columns() {
        return columns;
    }

    @Override
    public int compareTo(@NotNull IndexRowWrapper o) {
        int sizeCompare = Integer.compare(columns.length, o.columns.length);

        if (sizeCompare != 0) {
            return sizeCompare;
        }

        for (int i = 0; i < columns.length; ++i) {
            Comparator<Object> comparator = comparator(columns[i].getClass());

            int compare = comparator.compare(columns[i], o.columns[i]);

            if (compare != 0) {
                boolean asc = descriptor.indexRowColumns().get(i).asc();

                return asc ? compare : -compare;
            }
        }

        return 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IndexRowWrapper that = (IndexRowWrapper) o;
        return row.equals(that.row);
    }

    @Override
    public int hashCode() {
        return Objects.hash(row);
    }

    /**
     * Compares values generated by {@link SchemaTestUtils#generateRandomValue}.
     */
    private static Comparator<Object> comparator(Class<?> type) {
        if (Comparable.class.isAssignableFrom(type)) {
            return comparingNull(Comparable.class::cast, Comparator.naturalOrder());
        } else if (type.isArray()) {
            return comparingNull(Function.identity(), comparing(IndexRowWrapper::toBoxedArray, Arrays::compare));
        } else if (BitSet.class.isAssignableFrom(type)) {
            return comparingNull(BitSet.class::cast, comparing(BitSet::toLongArray, Arrays::compare));
        } else {
            throw new IllegalArgumentException("Non comparable class: " + type);
        }
    }

    /**
     * Creates a new array of boxed primitives if the given Object is an array of primitives or simply copies the array otherwise.
     */
    private static Comparable[] toBoxedArray(Object array) {
        return IntStream.range(0, Array.getLength(array))
                .mapToObj(i -> Array.get(array, i))
                .map(Comparable.class::cast)
                .toArray(Comparable[]::new);
    }
}
