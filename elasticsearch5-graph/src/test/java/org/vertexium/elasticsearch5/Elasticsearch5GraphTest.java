package org.vertexium.elasticsearch5;

import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsAction;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.io.stream.NotSerializableExceptionWrapper;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.vertexium.*;
import org.vertexium.elasticsearch5.scoring.ElasticsearchFieldValueScoringStrategy;
import org.vertexium.elasticsearch5.scoring.ElasticsearchHammingDistanceScoringStrategy;
import org.vertexium.inmemory.InMemoryAuthorizations;
import org.vertexium.query.QueryResultsIterable;
import org.vertexium.scoring.ScoringStrategy;
import org.vertexium.test.GraphTestBase;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.vertexium.test.util.VertexiumAssert.assertResultsCount;
import static org.vertexium.util.IterableUtils.toList;

public class Elasticsearch5GraphTest extends GraphTestBase {
    @ClassRule
    public static ElasticsearchResource elasticsearchResource = new ElasticsearchResource(Elasticsearch5GraphTest.class.getName());

    @Override
    protected Authorizations createAuthorizations(String... auths) {
        return getGraph().createAuthorizations(auths);
    }

    @Override
    protected Authorizations createButDontAddAuthorizations(String... auths) {
        return new InMemoryAuthorizations(auths);
    }

    @Before
    @Override
    public void before() throws Exception {
        elasticsearchResource.dropIndices();
        super.before();
    }

    @Override
    protected Graph createGraph() {
        Elasticsearch5GraphConfiguration configuration = new Elasticsearch5GraphConfiguration(elasticsearchResource.createConfig());
        return Elasticsearch5Graph.create(configuration);
    }

    private Elasticsearch5GraphSearchIndex getSearchIndex() {
        return (Elasticsearch5GraphSearchIndex) graph.getSearchIndex();
    }

    @Override
    protected boolean isFieldNamesInQuerySupported() {
        return true;
    }

    @Override
    protected boolean isParitalUpdateOfVertexPropertyKeySupported() {
        return true;
    }

    // TODO
//    @Override
//    protected boolean disableEdgeIndexing(Graph graph) {
//        Elasticsearch5SearchIndex searchIndex = (Elasticsearch5SearchIndex) graph.getSearchIndex();
//        searchIndex.getConfig().getGraphConfiguration().set(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + ElasticsearchSearchIndexConfiguration.INDEX_EDGES, "false");
//        return true;
//    }

    @Override
    protected boolean isLuceneQueriesSupported() {
        return true;
    }

    // TODO
//    @Test
//    @Override
//    public void testGraphQuerySortOnPropertyThatHasNoValuesInTheIndex() {
//        super.testGraphQuerySortOnPropertyThatHasNoValuesInTheIndex();
//
//        getSearchIndex().clearCache();
//
//        QueryResultsIterable<Vertex> vertices = graph.query(AUTHORIZATIONS_A).sort("age", SortDirection.ASCENDING).vertices();
//        Assert.assertEquals(2, count(vertices));
//    }

    @Override
    protected boolean isPainlessDateMath() {
        return true;
    }

    @Override
    protected boolean isDefaultSearchIndex() {
        return false;
    }

    // TODO
//    @Test
//    public void testQueryExecutionCountWhenPaging() {
//        graph.prepareVertex("v1", VISIBILITY_A).save(AUTHORIZATIONS_A);
//        graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A);
//        graph.flush();
//
//        long startingNumQueries = getNumQueries();
//
//        QueryResultsIterable<Vertex> vertices = graph.query(AUTHORIZATIONS_A).vertices();
//        assertEquals(startingNumQueries, getNumQueries());
//
//        assertResultsCount(2, 2, vertices);
//        assertEquals(startingNumQueries + 2, getNumQueries());
//
//        vertices = graph.query(AUTHORIZATIONS_A).limit(1).vertices();
//        assertEquals(startingNumQueries + 4, getNumQueries());
//
//        assertResultsCount(1, 2, vertices);
//        assertEquals(startingNumQueries + 4, getNumQueries());
//
//        vertices = graph.query(AUTHORIZATIONS_A).limit(10).vertices();
//        assertEquals(startingNumQueries + 6, getNumQueries());
//
//        assertResultsCount(2, 2, vertices);
//        assertEquals(startingNumQueries + 6, getNumQueries());
//    }

    // TODO
//    @Test
//    public void testQueryExecutionCountWhenScrollingApi() {
//        Elasticsearch5SearchIndex searchIndex = getSearchIndex();
//
//        searchIndex.getConfig().getGraphConfiguration().set(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + ElasticsearchSearchIndexConfiguration.QUERY_PAGE_SIZE, 1);
//
//        graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
//        graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A);
//        graph.flush();
//
//        long startingNumQueries = getNumQueries();
//
//        QueryResultsIterable<Vertex> vertices = graph.query(AUTHORIZATIONS_A).vertices();
//        assertResultsCount(2, vertices);
//        assertEquals(startingNumQueries + 4, getNumQueries());
//
//        searchIndex = (Elasticsearch5SearchIndex) graph.getSearchIndex();
//        searchIndex.getConfig().getGraphConfiguration().set(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + ElasticsearchSearchIndexConfiguration.QUERY_PAGE_SIZE, 2);
//
//        graph.addVertex("v3", VISIBILITY_A, AUTHORIZATIONS_A);
//        graph.flush();
//
//        vertices = graph.query(AUTHORIZATIONS_A).vertices();
//        assertResultsCount(3, vertices);
//        assertEquals(startingNumQueries + 8, getNumQueries());
//    }

    @Test
    public void testDisallowLeadingWildcardsInQueryString() {
        graph.prepareVertex("v1", VISIBILITY_A).setProperty("prop1", "value1", VISIBILITY_A).save(AUTHORIZATIONS_A);
        graph.flush();

        try {
            graph.query("*alue1", AUTHORIZATIONS_A).search().getTotalHits();
            fail("Wildcard prefix of query string should have caused an exception");
        } catch (Exception e) {
            if (!(getRootCause(e) instanceof NotSerializableExceptionWrapper)) {
                fail("Wildcard prefix of query string should have caused a NotSerializableExceptionWrapper exception");
            }
        }
    }

    // TODO
//    @Test
//    public void testLimitingNumberOfQueryStringTerms() {
//        graph.prepareVertex("v1", VISIBILITY_A).setProperty("prop1", "value1", VISIBILITY_A).save(AUTHORIZATIONS_A);
//        graph.flush();
//
//        StringBuilder q = new StringBuilder();
//        for (int i = 0; i < getSearchIndex().getConfig().getMaxQueryStringTerms(); i++) {
//            q.append("jeff").append(i).append(" ");
//        }
//
//        // should succeed
//        graph.query(q.toString(), AUTHORIZATIONS_A).search().getTotalHits();
//
//        try {
//            q.append("done");
//            graph.query(q.toString(), AUTHORIZATIONS_A).search().getTotalHits();
//            fail("Exceeding max query terms should have thrown an exception");
//        } catch (VertexiumException e) {
//            // expected
//        }
//    }

    @Test
    public void testQueryReturningElasticsearchEdge() {
        graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge("e1", "v1", "v2", LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        graph.flush();

        QueryResultsIterable<Edge> edges = graph.query(AUTHORIZATIONS_A)
            .edges(FetchHints.NONE);

        assertResultsCount(1, 1, edges);
        Edge e1 = toList(edges).get(0);
        assertEquals(LABEL_LABEL1, e1.getLabel());
        assertEquals("v1", e1.getVertexId(Direction.OUT));
        assertEquals("v2", e1.getVertexId(Direction.IN));
        assertEquals("e1", e1.getId());
    }

    @Test
    public void testQueryReturningElasticsearchVertex() {
        graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addVertex("v2", VISIBILITY_B, AUTHORIZATIONS_B);
        graph.addEdge("e1", "v1", "v2", LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        graph.flush();

        QueryResultsIterable<Vertex> vertices = graph.query(AUTHORIZATIONS_B)
            .vertices(FetchHints.NONE);

        assertResultsCount(1, 1, vertices);
        Vertex vertex = toList(vertices).get(0);
        assertEquals("v2", vertex.getId());
    }

    @Test
    public void testRetrievingVerticesFromElasticsearchEdge() {
        graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge("e1", "v1", "v2", LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        graph.flush();

        QueryResultsIterable<Edge> edges = graph.query(AUTHORIZATIONS_A)
            .edges(FetchHints.NONE);

        assertResultsCount(1, 1, edges);
        toList(edges).get(0).getVertices(AUTHORIZATIONS_A);
    }

    // TODO
//    @Test
//    public void testUpdateVertexWithDeletedElasticsearchDocument() {
//        TestElasticsearch5ExceptionHandler.authorizations = AUTHORIZATIONS_A;
//
//        graph.prepareVertex("v1", VISIBILITY_A)
//            .addPropertyValue("k1", "prop1", "joe", VISIBILITY_A)
//            .save(AUTHORIZATIONS_A);
//        graph.flush();
//
//        Vertex v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
//        getSearchIndex().deleteElement(graph, v1, AUTHORIZATIONS_A);
//        graph.flush();
//
//        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
//        v1.prepareMutation()
//            .addPropertyValue("k1", "prop2", "bob", VISIBILITY_A)
//            .save(AUTHORIZATIONS_A);
//        graph.flush();
//
//        QueryResultsIterable<String> results = graph.query("joe", AUTHORIZATIONS_A).vertexIds();
//        assertIdsAnyOrder(results);
//
//        results = graph.query("bob", AUTHORIZATIONS_A).vertexIds();
//        assertIdsAnyOrder(results, "v1");
//    }

    private long getNumQueries() {
        Client client = elasticsearchResource.getRunner().client();
        NodesStatsResponse nodeStats = NodesStatsAction.INSTANCE.newRequestBuilder(client).get();

        List<NodeStats> nodes = nodeStats.getNodes();
        assertEquals(1, nodes.size());

        SearchStats searchStats = nodes.get(0).getIndices().getSearch();
        return searchStats.getTotal().getQueryCount();
    }

    private Throwable getRootCause(Throwable e) {
        if (e.getCause() == null) {
            return e;
        }
        return getRootCause(e.getCause());
    }

    @Override
    protected ScoringStrategy getHammingDistanceScoringStrategy(String field, String hash) {
        return new ElasticsearchHammingDistanceScoringStrategy(field, hash);
    }

    @Override
    protected ScoringStrategy getFieldValueScoringStrategy(String field) {
        return new ElasticsearchFieldValueScoringStrategy(field);
    }

    @Override
    protected boolean isAddWithoutIndexingSupported() {
        return false;
    }

    public SourceAndFields[] getElementsInSearchIndex() {
        Client client = ((Elasticsearch5Graph) getGraph()).getClient();
        Stream<SearchHit> stream = Arrays.stream(client
            .prepareSearch("vertexium-test-edges", "vertexium-test-vertices")
            .setFetchSource(true)
            .storedFields("*")
            .get()
            .getHits()
            .getHits());
        return stream.map(sh -> new SourceAndFields(sh.getSource(), sh.getFields())).toArray(SourceAndFields[]::new);
    }

    public static class SourceAndFields {
        private final Map<String, Object> source;
        private final Map<String, SearchHitField> fields;

        public SourceAndFields(Map<String, Object> source, Map<String, SearchHitField> fields) {
            this.source = source;
            this.fields = fields;
        }

        @Override
        public String toString() {
            return source.get(FieldNames.ELEMENT_TYPE) + ": " + source.get(FieldNames.ELEMENT_ID);
        }
    }
}