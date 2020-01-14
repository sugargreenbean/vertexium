package org.vertexium.elasticsearch5.bulk;

import org.vertexium.ElementId;

public abstract class BulkItem {
    private final String indexName;
    private final ElementId elementId;
    private final String type;
    private final String documentId;
    private final long createdTime;
    private final BulkItemCompletableFuture addedToBatchFuture;
    private final BulkItemCompletableFuture completedFuture;
    private final StackTraceElement[] stackTrace;
    private long createdOrLastTriedTime;
    private int failCount;

    public BulkItem(
        String indexName,
        String type,
        String documentId,
        ElementId elementId
    ) {
        this.indexName = indexName;
        this.type = type;
        this.documentId = documentId;
        this.elementId = elementId;

        this.addedToBatchFuture = new BulkItemCompletableFuture(this);
        this.completedFuture = new BulkItemCompletableFuture(this);
        this.createdOrLastTriedTime = this.createdTime = System.currentTimeMillis();
        if (BulkUpdateService.LOGGER_STACK_TRACE.isTraceEnabled()) {
            this.stackTrace = Thread.currentThread().getStackTrace();
        } else {
            this.stackTrace = null;
        }
    }

    public String getIndexName() {
        return indexName;
    }

    public ElementId getElementId() {
        return elementId;
    }

    public String getType() {
        return type;
    }

    public String getDocumentId() {
        return documentId;
    }

    public abstract int getSize();

    public long getCreatedTime() {
        return createdTime;
    }

    public long getCreatedOrLastTriedTime() {
        return createdOrLastTriedTime;
    }

    public void updateLastTriedTime() {
        this.createdOrLastTriedTime = System.currentTimeMillis();
    }

    public void incrementFailCount() {
        failCount++;
    }

    public int getFailCount() {
        return failCount;
    }

    public BulkItemCompletableFuture getAddedToBatchFuture() {
        return addedToBatchFuture;
    }

    public BulkItemCompletableFuture getCompletedFuture() {
        return completedFuture;
    }

    public StackTraceElement[] getStackTrace() {
        return stackTrace;
    }

    @Override
    public String toString() {
        return String.format("%s {elementId=%s}", getClass().getSimpleName(), elementId);
    }
}
