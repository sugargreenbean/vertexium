package org.vertexium.elasticsearch7.bulk;

import java.util.concurrent.CompletableFuture;

public class BulkItemCompletableFuture extends CompletableFuture<Void> {
    private final BulkItem bulkItem;

    public BulkItemCompletableFuture(BulkItem bulkItem) {
        this.bulkItem = bulkItem;
    }

    public BulkItem getBulkItem() {
        return bulkItem;
    }

    @Override
    public String toString() {
        return String.format("BulkItemCompletableFuture{bulkItem=%s,%s}", bulkItem, super.toString());
    }
}
