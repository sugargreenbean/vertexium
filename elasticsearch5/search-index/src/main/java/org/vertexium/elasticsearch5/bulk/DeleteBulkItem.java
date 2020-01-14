package org.vertexium.elasticsearch5.bulk;

import org.vertexium.ElementId;

public class DeleteBulkItem extends BulkItem {
    private final int size;

    public DeleteBulkItem(
        String indexName,
        String type,
        String docId,
        ElementId elementId
    ) {
        super(indexName, type, docId, elementId);
        size = getIndexName().length()
            + getType().length()
            + getDocumentId().length()
            + getElementId().getId().length();
    }

    @Override
    public int getSize() {
        return size;
    }

    @Override
    public String toString() {
        return String.format("DeleteBulkItem {docId='%s'}", getDocumentId());
    }
}
