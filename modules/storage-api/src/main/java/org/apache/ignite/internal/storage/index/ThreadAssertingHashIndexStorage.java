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

package org.apache.ignite.internal.storage.index;

import static org.apache.ignite.internal.worker.ThreadAssertions.assertThreadAllowsToWrite;

import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.worker.ThreadAssertions;

/**
 * {@link HashIndexStorage} that performs thread assertions when doing read/write operations.
 *
 * @see ThreadAssertions
 */
public class ThreadAssertingHashIndexStorage extends ThreadAssertingIndexStorage implements HashIndexStorage {
    private final HashIndexStorage indexStorage;

    /** Constructor. */
    public ThreadAssertingHashIndexStorage(HashIndexStorage indexStorage) {
        super(indexStorage);

        this.indexStorage = indexStorage;
    }

    @Override
    public StorageHashIndexDescriptor indexDescriptor() {
        return indexStorage.indexDescriptor();
    }

    @Override
    public void destroy() throws StorageException {
        assertThreadAllowsToWrite();

        indexStorage.destroy();
    }
}
