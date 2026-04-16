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

package org.apache.ignite.internal.table.distributed;

import static org.apache.ignite.internal.catalog.events.CatalogEvent.TABLE_DROP;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.event.EventListener;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lowwatermark.TestLowWatermark;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.storage.DataStorageManager;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.TableViewInternal;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TableDestructionCoordinatorTest {
    private static final long TIMEOUT_MS = 5_000;

    private TestLowWatermark lowWatermark;
    private CatalogService catalogService;
    private TableRegistry tableRegistry;
    private TableZoneCoordinator zoneCoordinator;
    private SchemaManager schemaManager;
    private DataStorageManager dataStorageMgr;
    private MetricManager metricManager;
    private ExecutorService ioExecutor;

    private TableDestructionCoordinator coordinator;

    @BeforeEach
    void setUp() {
        lowWatermark = new TestLowWatermark();
        catalogService = mock(CatalogService.class);
        tableRegistry = new TableRegistry();
        zoneCoordinator = mock(TableZoneCoordinator.class);
        schemaManager = mock(SchemaManager.class);
        dataStorageMgr = mock(DataStorageManager.class);
        metricManager = mock(MetricManager.class);
        ioExecutor = Executors.newSingleThreadExecutor();

        coordinator = new TableDestructionCoordinator(
                catalogService,
                lowWatermark,
                tableRegistry,
                zoneCoordinator,
                schemaManager,
                dataStorageMgr,
                metricManager,
                ioExecutor
        );
    }

    @AfterEach
    void tearDown() {
        coordinator.stop();
        ioExecutor.shutdownNow();
    }

    @Test
    void destroyTableOnLwmAdvance() {
        int tableId = 1;
        int catalogVersion = 5;

        TableViewInternal table = registerStartedTable(tableId);
        MvTableStorage storage = table.internalTable().storage();

        when(zoneCoordinator.stopAndDestroyTableProcessors(any())).thenReturn(nullCompletedFuture());
        when(storage.destroy()).thenReturn(nullCompletedFuture());
        when(catalogService.activeCatalogVersion(anyLong())).thenReturn(catalogVersion);

        coordinator.start();
        coordinator.enqueueDestructionEvent(catalogVersion, tableId);

        // Advance LWM — triggers destruction of queued tables.
        assertThat(lowWatermark.updateAndNotify(new HybridTimestamp(1000, 0)), willCompleteSuccessfully());

        verify(zoneCoordinator, timeout(TIMEOUT_MS)).stopAndDestroyTableProcessors(table);
        verify(storage, timeout(TIMEOUT_MS)).destroy();
        verify(schemaManager, timeout(TIMEOUT_MS)).dropRegistry(tableId);
    }

    @Test
    void eventsNotDrainedBeforeCatalogVersionReached() {
        int tableId = 1;
        int catalogVersion = 10;

        registerStartedTable(tableId);

        // LWM advances to catalog version 5, but the event is at version 10 — not drained yet.
        when(catalogService.activeCatalogVersion(anyLong())).thenReturn(5);

        coordinator.start();
        coordinator.enqueueDestructionEvent(catalogVersion, tableId);

        assertThat(lowWatermark.updateAndNotify(new HybridTimestamp(1000, 0)), willCompleteSuccessfully());

        verify(zoneCoordinator, never()).stopAndDestroyTableProcessors(any());
    }

    @Test
    void stopPreventsDestructionOnLwmAdvance() {
        int tableId = 1;
        int catalogVersion = 5;

        registerStartedTable(tableId);

        when(catalogService.activeCatalogVersion(anyLong())).thenReturn(catalogVersion);

        coordinator.start();
        coordinator.enqueueDestructionEvent(catalogVersion, tableId);
        coordinator.stop();

        // Advance LWM after stop — listener was removed, no destruction should happen.
        assertThat(lowWatermark.updateAndNotify(new HybridTimestamp(1000, 0)), willCompleteSuccessfully());

        verify(zoneCoordinator, never()).stopAndDestroyTableProcessors(any());
    }

    @Test
    void startRegistersTableDropListener() {
        coordinator.start();

        verify(catalogService).listen(eq(TABLE_DROP), any(EventListener.class));
    }

    @Test
    void stopUnregistersTableDropListener() {
        coordinator.start();
        coordinator.stop();

        verify(catalogService).removeListener(eq(TABLE_DROP), any(EventListener.class));
    }

    @Test
    void cleanUpOnRecoveryDestroysOrphanStorages() {
        StorageEngine engine = mock(StorageEngine.class);
        when(engine.tableIdsOnDisk()).thenReturn(Set.of(1, 2, 3));
        when(engine.name()).thenReturn("test-engine");
        when(dataStorageMgr.allStorageEngines()).thenReturn(List.of(engine));

        // Only table 2 is alive (earliest version = latest version, catalog has only table 2).
        when(catalogService.earliestCatalogVersion()).thenReturn(1);
        when(catalogService.latestCatalogVersion()).thenReturn(1);

        var catalog = mock(org.apache.ignite.internal.catalog.Catalog.class);
        var tableDescriptor = mock(org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor.class);
        when(tableDescriptor.id()).thenReturn(2);
        when(catalog.tables()).thenReturn(List.of(tableDescriptor));
        when(catalogService.catalog(1)).thenReturn(catalog);

        coordinator.cleanUpOnRecovery();

        // Tables 1 and 3 are orphans — should be destroyed.
        verify(engine).destroyMvTable(1);
        verify(engine).destroyMvTable(3);
        verify(engine, never()).destroyMvTable(2);
    }

    private TableViewInternal registerStartedTable(int tableId) {
        TableViewInternal table = mock(TableViewInternal.class);
        InternalTable internalTable = mock(InternalTable.class);
        MvTableStorage storage = mock(MvTableStorage.class);

        when(table.internalTable()).thenReturn(internalTable);
        when(table.tableId()).thenReturn(tableId);
        when(table.name()).thenReturn("test_table_" + tableId);
        when(internalTable.storage()).thenReturn(storage);

        tableRegistry.register(tableId, table);
        tableRegistry.markStarted(tableId);

        return table;
    }
}
