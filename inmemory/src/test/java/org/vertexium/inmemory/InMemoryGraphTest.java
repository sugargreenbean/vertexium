package org.vertexium.inmemory;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.vertexium.*;
import org.vertexium.id.UUIDIdGenerator;
import org.vertexium.search.DefaultSearchIndex;
import org.vertexium.test.GraphTestBase;
import org.vertexium.test.TestMetadataPlugin;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class InMemoryGraphTest extends GraphTestBase {
    @Override
    protected Graph createGraph() {
        Map<String, String> config = createConfig();
        return new GraphFactory().createGraph(config);
    }

    private Map<String, String> createConfig() {
        Map<String, String> config = new HashMap<>();
        config.put("", InMemoryGraph.class.getName());
        config.put(GraphConfiguration.IDGENERATOR_PROP_PREFIX, UUIDIdGenerator.class.getName());
        config.put(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX, DefaultSearchIndex.class.getName());
        config.put(GraphConfiguration.METADATA_PLUGIN, TestMetadataPlugin.class.getName());
        return config;
    }

    @Override
    public InMemoryGraph getGraph() {
        return (InMemoryGraph) super.getGraph();
    }

    @Override
    protected Authorizations createAuthorizations(String... auths) {
        return new InMemoryAuthorizations(auths);
    }

    @Override
    protected void addAuthorizations(String... authorizations) {
        getGraph().createAuthorizations(authorizations);
    }

    @Before
    @Override
    public void before() throws Exception {
        super.before();
    }

    @After
    public void after() throws Exception {
        super.after();
    }

    @Override
    protected boolean isAdvancedGeoQuerySupported() {
        return false;
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testStrictTyping() {
        Map<String, String> config = createConfig();
        config.put(GraphConfiguration.STRICT_TYPING, "true");
        InMemoryGraph g = InMemoryGraph.create((Map) config);

        g.defineProperty("prop1").dataType(String.class).define();

        Vertex v = g.prepareVertex("v1", VISIBILITY_A)
            .addPropertyValue("k1", "prop1", "value1", VISIBILITY_A)
            .save(AUTHORIZATIONS_A);
        try {
            v.prepareMutation()
                .addPropertyValue("k1", "prop2", "value1", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
            throw new RuntimeException("Expected a type exception");
        } catch (VertexiumTypeException ex) {
            assertEquals("prop2", ex.getName());
            assertEquals(String.class, ex.getValueClass());
        }
    }

    @Override
    protected boolean isFetchHintNoneVertexQuerySupported() {
        return false;
    }
}
