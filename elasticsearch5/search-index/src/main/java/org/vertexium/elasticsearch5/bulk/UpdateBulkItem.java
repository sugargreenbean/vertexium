package org.vertexium.elasticsearch5.bulk;

import org.vertexium.ElementId;
import org.vertexium.ElementLocation;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import java.util.Collection;
import java.util.Date;
import java.util.Map;

public class UpdateBulkItem extends BulkItem {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(UpdateBulkItem.class);
    private final String extendedDataTableName;
    private final String extendedDataRowId;
    private final Map<String, String> source;
    private final Map<String, Object> fieldsToSet;
    private final Collection<String> fieldsToRemove;
    private final Map<String, String> fieldsToRename;
    private final Collection<String> additionalVisibilities;
    private final Collection<String> additionalVisibilitiesToDelete;
    private final boolean existingElement;
    private final int size;

    public UpdateBulkItem(
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
        this(
            indexName,
            type,
            docId,
            elementId,
            null,
            null,
            source,
            fieldsToSet,
            fieldsToRemove,
            fieldsToRename,
            additionalVisibilities,
            additionalVisibilitiesToDelete,
            existingElement
        );
    }

    public UpdateBulkItem(
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
        super(indexName, type, docId, elementId);
        this.extendedDataTableName = extendedDataTableName;
        this.extendedDataRowId = extendedDataRowId;
        this.source = source;
        this.fieldsToSet = fieldsToSet;
        this.fieldsToRemove = fieldsToRemove;
        this.fieldsToRename = fieldsToRename;
        this.additionalVisibilities = additionalVisibilities;
        this.additionalVisibilitiesToDelete = additionalVisibilitiesToDelete;
        this.existingElement = existingElement;

        int size = getIndexName().length()
            + getType().length()
            + getDocumentId().length()
            + getElementId().getId().length();
        for (Map.Entry<String, String> entry : source.entrySet()) {
            size += entry.getKey().length() + entry.getValue().length();
        }
        for (Map.Entry<String, Object> entry : fieldsToSet.entrySet()) {
            size += entry.getKey().length() + calculateSizeOfValue(entry.getValue());
        }
        for (String s : fieldsToRemove) {
            size += s.length();
        }
        for (Map.Entry<String, String> entry : fieldsToRename.entrySet()) {
            size += entry.getKey().length() + entry.getValue().length();
        }
        for (String s : additionalVisibilities) {
            size += s.length();
        }
        for (String s : additionalVisibilitiesToDelete) {
            size += s.length();
        }
        this.size = size;
    }

    private int calculateSizeOfValue(Object value) {
        if (value instanceof String) {
            return ((String) value).length();
        } else if (value instanceof Boolean) {
            return 4;
        } else if (value instanceof Number || value instanceof Date) {
            return 8;
        } else if (value instanceof Collection) {
            return ((Collection<?>) value).stream()
                .map(this::calculateSizeOfValue)
                .reduce(0, Integer::sum);
        } else if (value instanceof Map) {
            return ((Map<?, ?>) value).entrySet().stream()
                .map(entry -> calculateSizeOfValue(entry.getKey()) + calculateSizeOfValue(entry.getValue()))
                .reduce(0, Integer::sum);
        } else {
            LOGGER.warn("unhandled object to calculate size for: " + value.getClass().getName() + ", defaulting to 100");
            return 100;
        }
    }

    @Override
    public int getSize() {
        return size;
    }

    public ElementLocation getElementLocation() {
        return (ElementLocation) getElementId();
    }

    public String getExtendedDataTableName() {
        return extendedDataTableName;
    }

    public String getExtendedDataRowId() {
        return extendedDataRowId;
    }

    public Map<String, String> getSource() {
        return source;
    }

    public Map<String, Object> getFieldsToSet() {
        return fieldsToSet;
    }

    public Collection<String> getFieldsToRemove() {
        return fieldsToRemove;
    }

    public Map<String, String> getFieldsToRename() {
        return fieldsToRename;
    }

    public Collection<String> getAdditionalVisibilities() {
        return additionalVisibilities;
    }

    public Collection<String> getAdditionalVisibilitiesToDelete() {
        return additionalVisibilitiesToDelete;
    }

    public boolean isExistingElement() {
        return existingElement;
    }

    @Override
    public String toString() {
        return String.format(
            "%s {%s:%s%s}",
            UpdateBulkItem.class.getSimpleName(),
            getElementId().getElementType(),
            getElementId().getId(),
            getExtendedDataTableName() == null ? "" : String.format(" (%s:%s)", getExtendedDataTableName(), getExtendedDataRowId())
        );
    }
}
