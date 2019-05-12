package org.vertexium.elasticsearch5;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.vertexium.*;
import org.vertexium.elasticsearch5.bulk.BulkUpdateService;
import org.vertexium.mutation.*;
import org.vertexium.type.GeoPoint;
import org.vertexium.type.GeoShape;

import java.util.*;
import java.util.stream.Collectors;

import static org.vertexium.elasticsearch5.Elasticsearch5SearchIndex.*;
import static org.vertexium.util.StreamUtils.stream;

class AddOrUpdateService {
    private final Graph graph;
    private final Elasticsearch5SearchIndex searchIndex;
    private final Client client;
    private final ElasticsearchSearchIndexConfiguration config;
    private final IndexService indexService;
    private final IdStrategy idStrategy;
    private final PropertyNameService propertyNameService;
    private final BulkUpdateService bulkUpdateService;

    public AddOrUpdateService(
        Graph graph,
        Elasticsearch5SearchIndex searchIndex,
        Client client,
        ElasticsearchSearchIndexConfiguration config,
        IndexService indexService,
        IdStrategy idStrategy,
        PropertyNameService propertyNameService,
        BulkUpdateService bulkUpdateService
    ) {
        this.graph = graph;
        this.searchIndex = searchIndex;
        this.client = client;
        this.config = config;
        this.indexService = indexService;
        this.idStrategy = idStrategy;
        this.propertyNameService = propertyNameService;
        this.bulkUpdateService = bulkUpdateService;
    }

    public <TElement extends Element> void addOrUpdateElement(ElementMutation<TElement> elementMutation) {
        if (MUTATION_LOGGER.isTraceEnabled()) {
            MUTATION_LOGGER.trace("updateElement: %s", elementMutation.getId());
        }

        if (!config.isIndexEdges() && elementMutation.getElementType() == ElementType.EDGE) {
            return;
        }

        bulkUpdateService.flushUntilElementIdIsComplete(elementMutation.getId());

        UpdateRequestBuilder updateRequestBuilder = prepareUpdateForMutation(graph, elementMutation);

        if (updateRequestBuilder != null) {
            addMutationPropertiesToIndex(graph, elementMutation);
            searchIndex.addActionRequestBuilderForFlush(elementMutation, updateRequestBuilder.request());

            if (elementMutation instanceof ExistingElementMutation) {
                ExistingElementMutation<TElement> existingElementMutation = (ExistingElementMutation<TElement>) elementMutation;
                TElement element = existingElementMutation.getElement();
                if (existingElementMutation.getNewElementVisibility() != null && element.getFetchHints().isIncludeExtendedDataTableNames()) {
                    ImmutableSet<String> extendedDataTableNames = element.getExtendedDataTableNames();
                    if (extendedDataTableNames != null && !extendedDataTableNames.isEmpty()) {
                        extendedDataTableNames.forEach(tableName ->
                            alterExtendedDataElementTypeVisibility(
                                graph,
                                elementMutation,
                                element.getExtendedData(tableName),
                                existingElementMutation.getOldElementVisibility(),
                                existingElementMutation.getNewElementVisibility()
                            ));
                    }
                }
            }

            if (config.isAutoFlush()) {
                searchIndex.flush(graph);
            }
        }
    }

    private <TElement extends Element> UpdateRequestBuilder prepareUpdateForMutation(
        Graph graph,
        ElementMutation<TElement> mutation
    ) {
        Map<String, String> fieldVisibilityChanges = getFieldVisibilityChanges(graph, mutation);
        List<String> fieldsToRemove = getFieldsToRemove(graph, mutation);
        Map<String, Object> fieldsToSet = getFieldsToSet(graph, mutation);
        Set<Visibility> additionalVisibilities = getAdditionalVisibilities(mutation);
        Set<Visibility> additionalVisibilitiesToDelete = getAdditionalVisibilitiesToDelete(mutation);
        searchIndex.ensureAdditionalVisibilitiesDefined(additionalVisibilities);

        String documentId = idStrategy.createElementDocId(mutation);
        String indexName = indexService.getIndexName(mutation);
        IndexInfo indexInfo = indexService.ensureIndexCreatedAndInitialized(indexName);
        return searchIndex.prepareUpdateFieldsOnDocument(
            indexInfo.getIndexName(),
            documentId,
            mutation,
            fieldsToSet,
            fieldsToRemove,
            fieldVisibilityChanges,
            additionalVisibilities,
            additionalVisibilitiesToDelete
        );
    }

    private <TElement extends Element> Map<String, String> getFieldVisibilityChanges(Graph graph, ElementMutation<TElement> mutation) {
        Map<String, String> fieldVisibilityChanges = new HashMap<>();

        stream(mutation.getAlterPropertyVisibilities())
            .filter(p -> p.getExistingVisibility() != null && !p.getExistingVisibility().equals(p.getVisibility()))
            .forEach(p -> {
                String oldFieldName = propertyNameService.addVisibilityToPropertyName(graph, p.getName(), p.getExistingVisibility());
                String newFieldName = propertyNameService.addVisibilityToPropertyName(graph, p.getName(), p.getVisibility());
                fieldVisibilityChanges.put(oldFieldName, newFieldName);

                PropertyDefinition propertyDefinition = indexService.getPropertyDefinition(graph, p.getName());
                if (GeoShape.class.isAssignableFrom(propertyDefinition.getDataType())) {
                    fieldVisibilityChanges.put(oldFieldName + GEO_PROPERTY_NAME_SUFFIX, newFieldName + GEO_PROPERTY_NAME_SUFFIX);

                    if (GeoPoint.class.isAssignableFrom(propertyDefinition.getDataType())) {
                        fieldVisibilityChanges.put(oldFieldName + GEO_POINT_PROPERTY_NAME_SUFFIX, newFieldName + GEO_POINT_PROPERTY_NAME_SUFFIX);
                    }
                }
            });

        if (mutation instanceof ExistingElementMutation) {
            ExistingElementMutation<TElement> existingElementMutation = (ExistingElementMutation<TElement>) mutation;
            if (existingElementMutation.getNewElementVisibility() != null) {
                String oldFieldName = propertyNameService.addVisibilityToPropertyName(graph, ELEMENT_TYPE_FIELD_NAME, existingElementMutation.getOldElementVisibility());
                String newFieldName = propertyNameService.addVisibilityToPropertyName(graph, ELEMENT_TYPE_FIELD_NAME, existingElementMutation.getNewElementVisibility());
                fieldVisibilityChanges.put(oldFieldName, newFieldName);
            }
        }

        return fieldVisibilityChanges;
    }


    private <TElement extends Element> IndexInfo addMutationPropertiesToIndex(Graph graph, ElementMutation<TElement> mutation) {
        IndexInfo indexInfo = indexService.addPropertiesToIndex(graph, mutation, mutation.getProperties());

        mutation.getMarkPropertyHiddenData().forEach(p -> {
            if (!indexService.isPropertyInIndex(graph, HIDDEN_PROPERTY_FIELD_NAME, p.getVisibility())) {
                String hiddenVisibilityPropertyName = propertyNameService.addVisibilityToPropertyName(graph, HIDDEN_PROPERTY_FIELD_NAME, p.getVisibility());
                indexService.addPropertyToIndex(graph, indexInfo, hiddenVisibilityPropertyName, p.getVisibility(), Boolean.class, false, false, false);
            }
        });

        mutation.getMarkHiddenData().forEach(h -> {
            String hiddenVisibilityPropertyName = propertyNameService.addVisibilityToPropertyName(graph, HIDDEN_VERTEX_FIELD_NAME, h.getVisibility());
            if (!indexService.isPropertyInIndex(graph, hiddenVisibilityPropertyName, h.getVisibility())) {
                indexService.addPropertyToIndex(graph, indexInfo, hiddenVisibilityPropertyName, h.getVisibility(), Boolean.class, false, false, false);
            }
        });

        stream(mutation.getAlterPropertyVisibilities())
            .filter(p -> p.getExistingVisibility() != null && !p.getExistingVisibility().equals(p.getVisibility()))
            .forEach(p -> {
                PropertyDefinition propertyDefinition = indexService.getPropertyDefinition(graph, p.getName());
                if (propertyDefinition != null) {
                    try {
                        indexService.addPropertyDefinitionToIndex(graph, indexInfo, p.getName(), p.getVisibility(), propertyDefinition);
                    } catch (Exception e) {
                        throw new VertexiumException("Unable to add property to index: " + p, e);
                    }
                }
            });

        if (mutation instanceof ExistingElementMutation) {
            ExistingElementMutation<TElement> existingElementMutation = (ExistingElementMutation<TElement>) mutation;
            TElement element = existingElementMutation.getElement();
            if (existingElementMutation.getNewElementVisibility() != null) {
                try {
                    String newFieldName = propertyNameService.addVisibilityToPropertyName(graph, ELEMENT_TYPE_FIELD_NAME, existingElementMutation.getNewElementVisibility());
                    indexService.addPropertyToIndex(graph, indexInfo, newFieldName, element.getVisibility(), String.class, false, false, false);
                } catch (Exception e) {
                    throw new VertexiumException("Unable to add new element type visibility to index", e);
                }
            }
        }
        return indexInfo;
    }

    private <TElement extends Element> Map<String, Object> getFieldsToSet(
        Graph graph,
        ElementMutation<TElement> mutation
    ) {
        Map<String, Object> fieldsToSet = new HashMap<>(indexService.getPropertiesAsFields(graph, mutation.getProperties()));

        if (mutation instanceof ExistingElementMutation) {
            Set<PropertyDescriptor> propertyValuesToSkip = new HashSet<>();
            mutation.getProperties().forEach(p -> propertyValuesToSkip.add(PropertyDescriptor.fromProperty(p)));
            mutation.getPropertyDeletes().forEach(p -> propertyValuesToSkip.add(PropertyDescriptor.fromPropertyDeleteMutation(p)));
            mutation.getPropertySoftDeletes().forEach(p -> propertyValuesToSkip.add(PropertyDescriptor.fromPropertySoftDeleteMutation(p)));

            TElement element = ((ExistingElementMutation<TElement>) mutation).getElement();
            mutation.getProperties().forEach(p ->
                indexService.addExistingValuesToFieldMap(graph, element, p.getName(), p.getVisibility(), fieldsToSet, propertyValuesToSkip));
            mutation.getPropertyDeletes().forEach(p ->
                indexService.addExistingValuesToFieldMap(graph, element, p.getName(), p.getVisibility(), fieldsToSet, propertyValuesToSkip));
            mutation.getPropertySoftDeletes().forEach(p ->
                indexService.addExistingValuesToFieldMap(graph, element, p.getName(), p.getVisibility(), fieldsToSet, propertyValuesToSkip));
        } else {
            // TODO blind deletes?
            // TODO blind soft deletes?
        }

        mutation.getMarkPropertyHiddenData().forEach(p -> {
            String hiddenVisibilityPropertyName = propertyNameService.addVisibilityToPropertyName(graph, HIDDEN_PROPERTY_FIELD_NAME, p.getVisibility());
            fieldsToSet.put(hiddenVisibilityPropertyName, true);
        });

        mutation.getMarkHiddenData().forEach(h -> {
            String hiddenVisibilityPropertyName = propertyNameService.addVisibilityToPropertyName(graph, HIDDEN_VERTEX_FIELD_NAME, h.getVisibility());
            fieldsToSet.put(hiddenVisibilityPropertyName, true);
        });

        return fieldsToSet;
    }

    private <TElement extends Element> Set<Visibility> getAdditionalVisibilities(ElementMutation<TElement> mutation) {
        Set<Visibility> results = new HashSet<>();
        for (AdditionalVisibilityAddMutation additionalVisibility : mutation.getAdditionalVisibilities()) {
            results.add(additionalVisibility.getAdditionalVisibility());
        }
        return results;
    }

    private <TElement extends Element> Set<Visibility> getAdditionalVisibilitiesToDelete(ElementMutation<TElement> mutation) {
        Set<Visibility> results = new HashSet<>();
        for (AdditionalVisibilityDeleteMutation additionalVisibilityDelete : mutation.getAdditionalVisibilityDeletes()) {
            results.add(additionalVisibilityDelete.getAdditionalVisibility());
        }
        return results;
    }

    private <TElement extends Element> List<String> getFieldsToRemove(Graph graph, ElementMutation<TElement> mutation) {
        List<String> fieldsToRemove = new ArrayList<>();
        mutation.getPropertyDeletes().forEach(p -> fieldsToRemove.addAll(getRelatedFieldNames(graph, p.getName(), p.getVisibility())));
        mutation.getPropertySoftDeletes().forEach(p -> fieldsToRemove.addAll(getRelatedFieldNames(graph, p.getName(), p.getVisibility())));

        mutation.getMarkPropertyVisibleData().forEach(p -> {
            String hiddenVisibilityPropertyName = propertyNameService.addVisibilityToPropertyName(graph, HIDDEN_PROPERTY_FIELD_NAME, p.getVisibility());
            if (indexService.isPropertyInIndex(graph, HIDDEN_PROPERTY_FIELD_NAME, p.getVisibility())) {
                fieldsToRemove.add(hiddenVisibilityPropertyName);
            }
        });

        mutation.getMarkVisibleData().forEach(v -> {
            String hiddenVisibilityPropertyName = propertyNameService.addVisibilityToPropertyName(graph, HIDDEN_VERTEX_FIELD_NAME, v.getVisibility());
            if (indexService.isPropertyInIndex(graph, hiddenVisibilityPropertyName, v.getVisibility())) {
                fieldsToRemove.add(hiddenVisibilityPropertyName);
            }
        });

        return fieldsToRemove;
    }

    private List<String> getRelatedFieldNames(Graph graph, String name, Visibility visibility) {
        String propertyName = propertyNameService.addVisibilityToPropertyName(graph, name, visibility);

        List<String> fieldNames = new ArrayList<>();
        fieldNames.add(propertyName);

        PropertyDefinition propertyDefinition = indexService.getPropertyDefinition(graph, name);
        if (GeoShape.class.isAssignableFrom(propertyDefinition.getDataType())) {
            fieldNames.add(propertyName + GEO_PROPERTY_NAME_SUFFIX);

            if (GeoPoint.class.isAssignableFrom(propertyDefinition.getDataType())) {
                fieldNames.add(propertyName + GEO_POINT_PROPERTY_NAME_SUFFIX);
            }
        }
        return fieldNames;
    }

    public <T extends Element> void alterExtendedDataElementTypeVisibility(
        Graph graph,
        ElementMutation<T> elementMutation,
        Iterable<ExtendedDataRow> rows,
        Visibility oldVisibility,
        Visibility newVisibility
    ) {
        for (ExtendedDataRow row : rows) {
            String tableName = (String) row.getPropertyValue(ExtendedDataRow.TABLE_NAME);
            String rowId = (String) row.getPropertyValue(ExtendedDataRow.ROW_ID);
            String extendedDataDocId = idStrategy.createExtendedDataDocId(elementMutation, tableName, rowId);

            List<ExtendedDataMutation> columns = stream(row.getProperties())
                .map(property -> new ExtendedDataMutation(
                    tableName,
                    rowId,
                    property.getName(),
                    property.getKey(),
                    property.getValue(),
                    property.getTimestamp(),
                    property.getVisibility()
                )).collect(Collectors.toList());

            IndexInfo indexInfo = indexService.addExtendedDataColumnsToIndex(graph, elementMutation, tableName, rowId, columns);

            String oldElementTypeVisibilityPropertyName = propertyNameService.addVisibilityToPropertyName(graph, ELEMENT_TYPE_FIELD_NAME, oldVisibility);
            String newElementTypeVisibilityPropertyName = propertyNameService.addVisibilityToPropertyName(graph, ELEMENT_TYPE_FIELD_NAME, newVisibility);
            Map<String, String> fieldsToRename = Collections.singletonMap(oldElementTypeVisibilityPropertyName, newElementTypeVisibilityPropertyName);

            UpdateRequest request = client
                .prepareUpdate(indexInfo.getIndexName(), idStrategy.getType(), extendedDataDocId)
                .setScript(new Script(
                    ScriptType.STORED,
                    "painless",
                    "updateFieldsOnDocumentScript",
                    ImmutableMap.of(
                        "fieldsToSet", Collections.emptyMap(),
                        "fieldsToRemove", Collections.emptyList(),
                        "fieldsToRename", fieldsToRename,
                        "additionalVisibilities", Collections.emptyList(),
                        "additionalVisibilitiesToDelete", Collections.emptyList()
                    )
                ))
                .setRetryOnConflict(searchIndex.getConfig().getRetriesOnConflict())
                .request();
            bulkUpdateService.addUpdate(elementMutation, tableName, rowId, request);
        }
    }
}
