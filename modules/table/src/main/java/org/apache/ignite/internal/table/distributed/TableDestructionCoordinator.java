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

import static org.apache.ignite.internal.event.EventListener.fromConsumer;
import static org.apache.ignite.internal.table.distributed.TableUtils.aliveTables;
import static org.apache.ignite.internal.util.CollectionUtils.difference;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.hasCause;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockAsync;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.DropTableEventParameters;
import org.apache.ignite.internal.event.EventListener;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.lowwatermark.LowWatermark;
import org.apache.ignite.internal.lowwatermark.event.ChangeLowWatermarkEventParameters;
import org.apache.ignite.internal.lowwatermark.event.LowWatermarkEvent;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.storage.DataStorageManager;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.storage.metrics.StorageEngineTablesMetricSource;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.table.metrics.TableMetricSource;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.LongPriorityQueue;
import org.apache.ignite.table.QualifiedName;

/**
 * Coordinates table destruction operations.
 *
 * <p>Handles the deferred destruction of tables: when a table is dropped, the destruction is queued and only executed
 * once the low watermark advances past the catalog version of the drop event. This ensures that in-flight transactions
 * can still access the table data until it is safe to remove.
 */
final class TableDestructionCoordinator {
    private static final IgniteLogger LOG = Loggers.forClass(TableDestructionCoordinator.class);

    private final LongPriorityQueue<DestroyTableEvent> destructionEventsQueue =
            new LongPriorityQueue<>(DestroyTableEvent::catalogVersion);

    private final EventListener<DropTableEventParameters> onTableDropListener = fromConsumer(this::onTableDrop);
    private final EventListener<ChangeLowWatermarkEventParameters> onLowWatermarkChangedListener = this::onLwmChanged;

    private final CatalogService catalogService;
    private final LowWatermark lowWatermark;
    private final TableRegistry tableRegistry;
    private final TableZoneCoordinator zoneCoordinator;
    private final SchemaManager schemaManager;
    private final DataStorageManager dataStorageMgr;
    private final MetricManager metricManager;
    private final ExecutorService ioExecutor;
    private final IgniteSpinBusyLock busyLock;

    /**
     * Constructor.
     *
     * @param catalogService Catalog service.
     * @param lowWatermark Low watermark.
     * @param tableRegistry Table registry.
     * @param zoneCoordinator Zone coordinator.
     * @param schemaManager Schema manager.
     * @param dataStorageMgr Data storage manager.
     * @param metricManager Metric manager.
     * @param ioExecutor Executor for IO operations.
     * @param busyLock Busy lock shared with TableManager.
     */
    TableDestructionCoordinator(
            CatalogService catalogService,
            LowWatermark lowWatermark,
            TableRegistry tableRegistry,
            TableZoneCoordinator zoneCoordinator,
            SchemaManager schemaManager,
            DataStorageManager dataStorageMgr,
            MetricManager metricManager,
            ExecutorService ioExecutor,
            IgniteSpinBusyLock busyLock
    ) {
        this.catalogService = catalogService;
        this.lowWatermark = lowWatermark;
        this.tableRegistry = tableRegistry;
        this.zoneCoordinator = zoneCoordinator;
        this.schemaManager = schemaManager;
        this.dataStorageMgr = dataStorageMgr;
        this.metricManager = metricManager;
        this.ioExecutor = ioExecutor;
        this.busyLock = busyLock;
    }

    /**
     * Registers catalog and low watermark event listeners.
     */
    void start() {
        catalogService.listen(CatalogEvent.TABLE_DROP, onTableDropListener);
        lowWatermark.listen(LowWatermarkEvent.LOW_WATERMARK_CHANGED, onLowWatermarkChangedListener);
    }

    /**
     * Unregisters event listeners.
     */
    void stop() {
        lowWatermark.removeListener(LowWatermarkEvent.LOW_WATERMARK_CHANGED, onLowWatermarkChangedListener);
        catalogService.removeListener(CatalogEvent.TABLE_DROP, onTableDropListener);
    }

    /**
     * Cleans up resources for tables that were dropped before node recovery.
     * Must be called before {@link #start()} to handle orphan storages from previous runs.
     */
    void cleanUpOnRecovery() {
        // TODO: IGNITE-20384 Clean up abandoned resources for dropped tables from vault and metastore
        Set<Integer> aliveTableIds = aliveTables(catalogService, lowWatermark.getLowWatermark());
        destroyMvStoragesForTablesNotIn(aliveTableIds);
    }

    /**
     * Enqueues a table destruction event. Called during table recovery to handle missed drop events.
     *
     * @param catalogVersion Catalog version of the drop event.
     * @param tableId Table identifier.
     */
    void enqueueDestructionEvent(int catalogVersion, int tableId) {
        destructionEventsQueue.enqueue(new DestroyTableEvent(catalogVersion, tableId));
    }

    private void onTableDrop(DropTableEventParameters parameters) {
        inBusyLock(busyLock, () -> {
            unregisterMetricsSource(tableRegistry.startedTable(parameters.tableId()));

            destructionEventsQueue.enqueue(new DestroyTableEvent(parameters.catalogVersion(), parameters.tableId()));
        });
    }

    // TODO https://issues.apache.org/jira/browse/IGNITE-27468 Not "thread-safe" in case of concurrent disaster recovery or rebalances.
    private CompletableFuture<Boolean> onLwmChanged(ChangeLowWatermarkEventParameters parameters) {
        if (!busyLock.enterBusy()) {
            return falseCompletedFuture();
        }

        try {
            int newEarliestCatalogVersion = catalogService.activeCatalogVersion(parameters.newLowWatermark().longValue());

            // Run table destruction fully asynchronously.
            destructionEventsQueue.drainUpTo(newEarliestCatalogVersion)
                    .forEach(event -> destroyTableLocally(event.tableId()));

            return falseCompletedFuture();
        } catch (Throwable t) {
            return CompletableFuture.failedFuture(t);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Drops local structures for a table.
     *
     * @param tableId Table id to destroy.
     */
    private CompletableFuture<Void> destroyTableLocally(int tableId) {
        TableViewInternal table = tableRegistry.removeStarted(tableId);

        assert table != null : tableId;

        InternalTable internalTable = table.internalTable();

        return zoneCoordinator.stopAndDestroyTableProcessors(table)
                .thenComposeAsync(unused -> inBusyLockAsync(busyLock, () -> internalTable.storage().destroy()), ioExecutor)
                .thenAccept(unused -> inBusyLock(busyLock, () -> {
                    tableRegistry.unregister(tableId);

                    schemaManager.dropRegistry(tableId);
                }))
                .whenComplete((v, e) -> {
                    if (e != null && !hasCause(e, NodeStoppingException.class)) {
                        LOG.error("Unable to destroy table [name={}, tableId={}]", e, table.name(), tableId);
                    }
                });
    }

    private void destroyMvStoragesForTablesNotIn(Set<Integer> aliveTableIds) {
        for (StorageEngine storageEngine : dataStorageMgr.allStorageEngines()) {
            Set<Integer> tableIdsOnDisk = storageEngine.tableIdsOnDisk();

            for (int tableId : difference(tableIdsOnDisk, aliveTableIds)) {
                storageEngine.destroyMvTable(tableId);
                LOG.info("Destroyed table MV storage for table {} in storage engine '{}'", tableId, storageEngine.name());
            }
        }
    }

    private void unregisterMetricsSource(TableViewInternal table) {
        if (table == null) {
            return;
        }

        QualifiedName tableName = table.qualifiedName();

        try {
            metricManager.unregisterSource(TableMetricSource.sourceName(tableName));
        } catch (Exception e) {
            LOG.warn("Failed to unregister metrics source for table [id={}, name={}].", e, table.tableId(), tableName);
        }

        String storageProfile = table.internalTable().storage().getTableDescriptor().getStorageProfile();
        StorageEngine engine = dataStorageMgr.engineByStorageProfile(storageProfile);

        // Engine can be null sometimes, see "TableManager.createTableStorage".
        if (engine != null) {
            try {
                metricManager.unregisterSource(StorageEngineTablesMetricSource.sourceName(engine.name(), tableName));
            } catch (Exception e) {
                LOG.warn("Failed to unregister storage engine metrics source for table [id={}, name={}].", e, table.tableId(), tableName);
            }
        }
    }

    /** Internal event. */
    private static class DestroyTableEvent {
        final int catalogVersion;
        final int tableId;

        DestroyTableEvent(int catalogVersion, int tableId) {
            this.catalogVersion = catalogVersion;
            this.tableId = tableId;
        }

        int catalogVersion() {
            return catalogVersion;
        }

        int tableId() {
            return tableId;
        }
    }
}
