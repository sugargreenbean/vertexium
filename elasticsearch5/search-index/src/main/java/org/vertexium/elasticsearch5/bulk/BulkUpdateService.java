package org.vertexium.elasticsearch5.bulk;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.vertexium.ElementId;
import org.vertexium.VertexiumException;
import org.vertexium.elasticsearch5.Elasticsearch5SearchIndex;
import org.vertexium.elasticsearch5.IndexRefreshTracker;
import org.vertexium.metric.Histogram;
import org.vertexium.metric.Timer;
import org.vertexium.metric.VertexiumMetricRegistry;
import org.vertexium.util.LimitedLinkedBlockingQueue;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * All updates to Elasticsearch are sent using bulk requests to speed up indexing.
 *
 * Duplicate element updates are collapsed into single updates to reduce the number of refreshes Elasticsearch
 * has to perform. See
 * - https://github.com/elastic/elasticsearch/issues/23792#issuecomment-296149685
 * - https://github.com/debadair/elasticsearch/commit/54cdf40bc5fdecce180ba2e242abca59c7bd1f11
 */
public class BulkUpdateService {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(BulkUpdateService.class);
    private static final String LOGGER_STACK_TRACE_NAME = BulkUpdateService.class.getName() + ".STACK_TRACE";
    static final VertexiumLogger LOGGER_STACK_TRACE = VertexiumLoggerFactory.getLogger(LOGGER_STACK_TRACE_NAME);
    private final Elasticsearch5SearchIndex searchIndex;
    private final IndexRefreshTracker indexRefreshTracker;
    private final LimitedLinkedBlockingQueue<BulkItem> incomingItems = new LimitedLinkedBlockingQueue<>();
    private final OutstandingItemsList outstandingItems = new OutstandingItemsList();
    private final Thread processItemsThread;
    private final Timer flushTimer;
    private final Histogram batchSizeHistogram;
    private final Timer flushUntilElementIdIsCompleteTimer;
    private final Timer processBatchTimer;
    private final Duration bulkRequestTimeout;
    private final ThreadPoolExecutor ioExecutor;
    private final int maxFailCount;
    private final BulkItemBatch batch;
    private volatile boolean shutdown;

    public BulkUpdateService(
        Elasticsearch5SearchIndex searchIndex,
        IndexRefreshTracker indexRefreshTracker,
        BulkUpdateServiceConfiguration configuration
    ) {
        this.searchIndex = searchIndex;
        this.indexRefreshTracker = indexRefreshTracker;

        this.ioExecutor = new ThreadPoolExecutor(
            configuration.getPoolSize(),
            configuration.getPoolSize(),
            10,
            TimeUnit.SECONDS,
            new LimitedLinkedBlockingQueue<>(configuration.getBacklogSize()),
            r -> {
                Thread thread = new Thread(r);
                thread.setDaemon(true);
                thread.setName("vertexium-es-processItems-io-" + thread.getId());
                return thread;
            }
        );

        this.processItemsThread = new Thread(this::processIncomingItemsIntoBatches);
        this.processItemsThread.setName("vertexium-es-processItems");
        this.processItemsThread.setDaemon(true);
        this.processItemsThread.start();

        this.bulkRequestTimeout = configuration.getBulkRequestTimeout();
        this.maxFailCount = configuration.getMaxFailCount();
        this.batch = new BulkItemBatch(
            configuration.getMaxBatchSize(),
            configuration.getMaxBatchSizeInBytes(),
            configuration.getBatchWindowTime(),
            configuration.getLogRequestSizeLimit()
        );

        VertexiumMetricRegistry metricRegistry = searchIndex.getMetricsRegistry();
        this.flushTimer = metricRegistry.getTimer(BulkUpdateService.class, "flush", "timer");
        this.processBatchTimer = metricRegistry.getTimer(BulkUpdateService.class, "processBatch", "timer");
        this.batchSizeHistogram = metricRegistry.getHistogram(BulkUpdateService.class, "batch", "histogram");
        this.flushUntilElementIdIsCompleteTimer = metricRegistry.getTimer(BulkUpdateService.class, "flushUntilElementIdIsComplete", "timer");
        metricRegistry.getGauge(metricRegistry.createName(BulkUpdateService.class, "outstandingItems", "size"), outstandingItems::size);
    }

    private void processIncomingItemsIntoBatches() {
        while (true) {
            try {
                if (shutdown) {
                    return;
                }

                BulkItem item = incomingItems.poll(100, TimeUnit.MILLISECONDS);
                if (batch.shouldFlushByTime()) {
                    flushBatch();
                }
                if (item == null) {
                    continue;
                }
                if (filterByRetryTime(item)) {
                    while (!batch.add(item)) {
                        flushBatch();
                    }
                    item.getAddedToBatchFuture().complete(null);
                }
            } catch (InterruptedException ex) {
                // we are shutting down so return
                return;
            } catch (Exception ex) {
                LOGGER.error("process items failed", ex);
            }
        }
    }

    private void flushBatch() {
        List<BulkItem> batchItems = batch.getItemsAndClear();
        if (batchItems.size() > 0) {
            ioExecutor.execute(() -> processBatch(batchItems));
        }
    }

    private boolean filterByRetryTime(BulkItem bulkItem) {
        if (bulkItem.getFailCount() == 0) {
            return true;
        }
        long nextRetryTime = (long) (bulkItem.getCreatedOrLastTriedTime() + (10 * Math.pow(2, bulkItem.getFailCount())));
        long currentTime = System.currentTimeMillis();
        if (nextRetryTime > currentTime) {
            // add it back into incomingItems, it will already be in outstandingItems
            incomingItems.add(bulkItem);
            return false;
        }
        return true;
    }

    private void processBatch(List<BulkItem> bulkItems) {
        processBatchTimer.time(() -> {
            try {
                batchSizeHistogram.update(bulkItems.size());

                BulkItemsToBulkRequestResult bulkItemsToBulkRequestResult = bulkItemsToBulkRequest(bulkItems);
                BulkResponse bulkResponse = searchIndex.getClient()
                    .bulk(bulkItemsToBulkRequestResult.bulkRequest)
                    .get(bulkRequestTimeout.toMillis(), TimeUnit.MILLISECONDS);

                Set<String> indexNames = bulkItems.stream()
                    .peek(BulkItem::updateLastTriedTime)
                    .map(BulkItem::getIndexName)
                    .collect(Collectors.toSet());
                indexRefreshTracker.pushChanges(indexNames);

                int itemIndex = 0;
                for (BulkItemResponse bulkItemResponse : bulkResponse.getItems()) {
                    List<BulkItem> bulkItemsForResponse = bulkItemsToBulkRequestResult.requestsToBulkItems.get(itemIndex++);
                    for (BulkItem bulkItem : bulkItemsForResponse) {
                        if (bulkItemResponse.isFailed()) {
                            handleFailure(bulkItem, bulkItemResponse);
                        } else {
                            handleSuccess(bulkItem);
                        }
                    }
                }
            } catch (Exception ex) {
                LOGGER.error("bulk request failed", ex);
                // if bulk failed try each item individually
                if (bulkItems.size() > 1) {
                    for (BulkItem bulkItem : bulkItems) {
                        processBatch(Lists.newArrayList(bulkItem));
                    }
                } else {
                    complete(bulkItems.get(0), ex);
                }
            }
        });
    }

    private void handleSuccess(BulkItem bulkItem) {
        complete(bulkItem, null);
    }

    private void handleFailure(BulkItem bulkItem, BulkItemResponse bulkItemResponse) {
        BulkItemResponse.Failure failure = bulkItemResponse.getFailure();
        bulkItem.incrementFailCount();
        if (bulkItem.getFailCount() >= maxFailCount) {
            complete(bulkItem, new BulkVertexiumException("fail count exceeded the max number of failures", failure));
        } else {
            AtomicBoolean retry = new AtomicBoolean(false);
            try {
                searchIndex.handleBulkFailure(bulkItem, bulkItemResponse, retry);
            } catch (Exception ex) {
                complete(bulkItem, ex);
                return;
            }
            if (retry.get()) {
                incomingItems.add(bulkItem);
            } else {
                complete(bulkItem, null);
            }
        }
    }

    private void complete(BulkItem bulkItem, Exception exception) {
        outstandingItems.remove(bulkItem);
        if (exception == null) {
            bulkItem.getCompletedFuture().complete(null);
        } else {
            bulkItem.getCompletedFuture().completeExceptionally(exception);
        }
    }

    private static class BulkItemsToBulkRequestResult {
        BulkRequest bulkRequest;
        List<List<BulkItem>> requestsToBulkItems;
    }

    private BulkItemsToBulkRequestResult bulkItemsToBulkRequest(List<BulkItem> bulkItems) {
        BulkItemsToBulkRequestResult results = new BulkItemsToBulkRequestResult();
        results.requestsToBulkItems = new ArrayList<>();
        BulkRequestBuilder builder = searchIndex.getClient().prepareBulk();

        Map<String, List<BulkItem>> byIndexName = bulkItems.stream().collect(Collectors.groupingBy(BulkItem::getIndexName));
        for (Map.Entry<String, List<BulkItem>> byIndexNameEntry : byIndexName.entrySet()) {
            String indexName = byIndexNameEntry.getKey();
            Map<String, List<BulkItem>> byType = byIndexNameEntry.getValue().stream().collect(Collectors.groupingBy(BulkItem::getType));

            for (Map.Entry<String, List<BulkItem>> byTypeEntry : byType.entrySet()) {
                String type = byTypeEntry.getKey();
                Map<String, List<BulkItem>> byDocumentId = byTypeEntry.getValue().stream().collect(Collectors.groupingBy(BulkItem::getDocumentId));

                for (Map.Entry<String, List<BulkItem>> byDocumentIdEntry : byDocumentId.entrySet()) {
                    String documentId = byDocumentIdEntry.getKey();
                    List<BulkItem> requestBulkItems = byDocumentIdEntry.getValue();
                    Map<Class<? extends BulkItem>, List<BulkItem>> byBulkItemType = requestBulkItems.stream().collect(Collectors.groupingBy(BulkItem::getClass));
                    if (byBulkItemType.size() > 1) {
                        throw new VertexiumException(
                            "Cannot mix bulk item types in a batch: "
                                + byBulkItemType.keySet().stream().map(Class::getName).collect(Collectors.joining(","))
                        );
                    }
                    for (Map.Entry<Class<? extends BulkItem>, List<BulkItem>> byBulkItemTypeEntry : byBulkItemType.entrySet()) {
                        Class<? extends BulkItem> bulkItemType = byBulkItemTypeEntry.getKey();
                        if (bulkItemType == UpdateBulkItem.class) {
                            Map<String, String> source = new HashMap<>();
                            Map<String, Object> fieldsToSet = new HashMap<>();
                            List<String> fieldsToRemove = new ArrayList<>();
                            Map<String, String> fieldsToRename = new HashMap<>();
                            List<String> additionalVisibilities = new ArrayList<>();
                            List<String> additionalVisibilitiesToDelete = new ArrayList<>();

                            boolean updateOnly = true;
                            for (BulkItem bulkItem : requestBulkItems) {
                                UpdateBulkItem updateBulkItem = (UpdateBulkItem) bulkItem;
                                source.putAll(updateBulkItem.getSource());
                                fieldsToSet.putAll(updateBulkItem.getFieldsToSet());
                                fieldsToRemove.addAll(updateBulkItem.getFieldsToRemove());
                                fieldsToRename.putAll(updateBulkItem.getFieldsToRename());
                                additionalVisibilities.addAll(updateBulkItem.getAdditionalVisibilities());
                                additionalVisibilitiesToDelete.addAll(updateBulkItem.getAdditionalVisibilitiesToDelete());
                                if (!updateBulkItem.isExistingElement()) {
                                    updateOnly = false;
                                }
                            }

                            UpdateRequestBuilder updateRequestBuilder = searchIndex.getClient()
                                .prepareUpdate(indexName, type, documentId);
                            if (!updateOnly) {
                                updateRequestBuilder = updateRequestBuilder
                                    .setScriptedUpsert(true)
                                    .setUpsert(source);
                            }
                            UpdateRequest updateRequest = updateRequestBuilder
                                .setScript(new Script(
                                    ScriptType.STORED,
                                    "painless",
                                    "updateFieldsOnDocumentScript",
                                    ImmutableMap.of(
                                        "fieldsToSet", fieldsToSet,
                                        "fieldsToRemove", fieldsToRemove,
                                        "fieldsToRename", fieldsToRename,
                                        "additionalVisibilities", additionalVisibilities,
                                        "additionalVisibilitiesToDelete", additionalVisibilitiesToDelete
                                    )
                                ))
                                .setRetryOnConflict(Elasticsearch5SearchIndex.MAX_RETRIES)
                                .request();
                            builder.add(updateRequest);
                            results.requestsToBulkItems.add(requestBulkItems);
                        } else if (bulkItemType == DeleteBulkItem.class) {
                            DeleteRequest deleteRequest = searchIndex.getClient()
                                .prepareDelete(indexName, type, documentId)
                                .request();
                            builder.add(deleteRequest);
                            results.requestsToBulkItems.add(requestBulkItems);
                        } else {
                            throw new VertexiumException("unhandled bulk request item: " + bulkItemType.getName());
                        }
                    }
                }
            }
        }

        results.bulkRequest = builder.request();
        return results;
    }

    public void flush() {
        flushTimer.time(() -> {
            try {
                List<BulkItem> items = outstandingItems.getItems();

                // wait for the items to be added to batches
                CompletableFuture.allOf(
                    items.stream()
                        .map(BulkItem::getAddedToBatchFuture)
                        .toArray(CompletableFuture[]::new)
                ).get();

                // flush the current batch
                flushBatch();

                // wait for the items to complete
                CompletableFuture.allOf(
                    items.stream()
                        .map(BulkItem::getCompletedFuture)
                        .toArray(CompletableFuture[]::new)
                ).get();
            } catch (Exception ex) {
                throw new VertexiumException("failed to flush", ex);
            }
        });
    }

    public CompletableFuture<Void> addUpdate(
        String indexName,
        String type,
        String docId,
        ElementId elementId,
        Map<String, String> source,
        Map<String, Object> fieldsToSet,
        Collection<String> fieldsToRemove,
        Map<String, String> fieldsToRename,
        Collection<String> additionalVisibilities,
        Collection<String> additionalVisibilitiesToDelete,
        boolean existingElement
    ) {
        return add(new UpdateBulkItem(
            indexName,
            type,
            docId,
            elementId,
            source,
            fieldsToSet,
            fieldsToRemove,
            fieldsToRename,
            additionalVisibilities,
            additionalVisibilitiesToDelete,
            existingElement
        ));
    }

    public CompletableFuture<Void> addUpdate(
        String indexName,
        String type,
        String docId,
        ElementId elementId,
        String extendedDataTableName,
        String extendedDataRowId,
        Map<String, String> source,
        Map<String, Object> fieldsToSet,
        Collection<String> fieldsToRemove,
        Map<String, String> fieldsToRename,
        Collection<String> additionalVisibilities,
        Collection<String> additionalVisibilitiesToDelete,
        boolean existingElement
    ) {
        return add(new UpdateBulkItem(
            indexName,
            type,
            docId,
            elementId,
            extendedDataTableName,
            extendedDataRowId,
            source,
            fieldsToSet,
            fieldsToRemove,
            fieldsToRename,
            additionalVisibilities,
            additionalVisibilitiesToDelete,
            existingElement
        ));
    }

    public CompletableFuture<Void> addDelete(
        String indexName,
        String type,
        String docId,
        ElementId elementId
    ) {
        return add(new DeleteBulkItem(indexName, type, docId, elementId));
    }

    private CompletableFuture<Void> add(BulkItem bulkItem) {
        outstandingItems.add(bulkItem);
        incomingItems.add(bulkItem);
        return bulkItem.getCompletedFuture();
    }

    public void shutdown() {
        this.shutdown = true;
        try {
            this.processItemsThread.join(10_000);
        } catch (InterruptedException e) {
            // OK
        }

        ioExecutor.shutdown();
    }

    public void flushUntilElementIdIsComplete(String elementId) {
        long startTime = System.currentTimeMillis();
        BulkItemCompletableFuture lastFuture = null;
        while (true) {
            BulkItem item = outstandingItems.getItemForElementId(elementId);
            if (item == null) {
                break;
            }
            lastFuture = item.getCompletedFuture();
            try {
                if (!item.getCompletedFuture().isDone()) {
                    item.getAddedToBatchFuture().get();
                    flushBatch();
                }
                item.getCompletedFuture().get();
            } catch (Exception ex) {
                throw new VertexiumException("Failed to flushUntilElementIdIsComplete: " + elementId, ex);
            }
        }
        long endTime = System.currentTimeMillis();
        long delta = endTime - startTime;
        flushUntilElementIdIsCompleteTimer.update(delta, TimeUnit.MILLISECONDS);
        if (delta > 1_000) {
            String message = String.format(
                "flush of %s got stuck for %dms%s",
                elementId,
                delta,
                LOGGER_STACK_TRACE.isTraceEnabled()
                    ? ""
                    : String.format(" (for more information enable trace level on \"%s\")", LOGGER_STACK_TRACE_NAME)
            );
            if (delta > 60_000) {
                LOGGER.error("%s", message);
            } else if (delta > 10_000) {
                LOGGER.warn("%s", message);
            } else {
                LOGGER.info("%s", message);
            }
            if (LOGGER_STACK_TRACE.isTraceEnabled()) {
                logStackTrace("Current stack trace", Thread.currentThread().getStackTrace());
                if (lastFuture != null) {
                    StackTraceElement[] stackTrace = lastFuture.getBulkItem().getStackTrace();
                    if (stackTrace != null) {
                        logStackTrace("Other stack trace causing the delay", stackTrace);
                    }
                }
            }
        }
    }

    private void logStackTrace(String message, StackTraceElement[] stackTrace) {
        if (!LOGGER_STACK_TRACE.isTraceEnabled()) {
            return;
        }
        LOGGER_STACK_TRACE.trace(
            "%s",
            message + "\n" +
                Arrays.stream(stackTrace)
                    .map(e -> "   " + e.toString())
                    .collect(Collectors.joining("\n"))
        );
    }
}
