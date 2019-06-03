package org.elasticsearch.module.graphite.test;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugin.graphite.GraphitePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.service.graphite.GraphiteService;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class GraphitePluginIntegrationITCase extends ESIntegTestCase{

    public static final int GRAPHITE_SERVER_PORT = 12345;

    private static GraphiteMockServer graphiteMockServer;
    private static String clusterName;
    private static String index;
    private static String type;

    @BeforeClass
    public static void startGraphiteMockServerAndNode() throws Exception {
        clusterName = "cluster" + randomShort();
        index = "index" + randomShort();
        type = "type" + randomShort();
        
        graphiteMockServer = new GraphiteMockServer(2003);
        SpecialPermission.check();
        AccessController.doPrivileged(new PrivilegedAction<Object>() {
            public Object run() {
                graphiteMockServer.start();
                return null;
            }
        });

        System.out.println("started");
    }

    @AfterClass
    public static void stopGraphiteServer() throws Exception {
        graphiteMockServer.close();
    }
    
    
    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal))
                .put("metrics.graphite.prefix", "elasticsearch." +  clusterName)
                .put("metrics.graphite.every", "1s")
                .put("metrics.graphite.host", "localhost")
                .put("metrics.graphite.perIndex", true)
                .put(GraphiteService.INCLUDE_INDEXES.getKey(), index)
                .put("metrics.graphite.port", "2003")
                .put("metrics.graphite.include", ".*\\.gc.old.*")
                .put("metrics.graphite.exclude", ".*\\.gc.*")
               .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>();
        plugins.add(GraphitePlugin.class);
        return plugins;
    }    

    @Test
    public void testThatIndexingResultsInMonitoring() throws Exception {
        IndexResponse indexResponse = indexElement(index, type, "value");
        assertThat(indexResponse.getId(), is(notNullValue()));
        SearchResponse searchResponse = searchElement();
        assertTrue(searchResponse != null);

        Thread.sleep(2000);

        ensureValidKeyNames();
        assertGraphiteMetricIsContained("^elasticsearch." + clusterName + ".indexes." + index + ".id...indexing._all.indexCount 1");
        assertGraphiteMetricIsContained("^elasticsearch." + clusterName + ".indexes." + index + ".id...search._all.queryCount .");
        assertGraphiteMetricIsContained("^elasticsearch." + clusterName + ".node.search._all.queryCount ");
    }

    @Test
    public void testThatFieldExclusionAndInclusionWorks() throws Exception {
        IndexResponse indexResponse = indexElement(index, type, "value"+randomShort());
        assertThat(indexResponse.getId(), is(notNullValue()));

        Thread.sleep(2000);

        ensureValidKeyNames();
        // ensure no global exclusion
        assertGraphiteMetricIsContained("elasticsearch." + clusterName + ".indexes." + index + ".id...indexing._all.indexCount .");
        assertGraphiteMetricIsContained("elasticsearch." + clusterName + ".node.jvm.gc.old.* ");
        assertGraphiteMetricIsNotContained("elasticsearch." + clusterName + ".node.jvm.gc.new.* ");
        
    }

    private boolean any(final Collection<String> collection, final String regex) {
        final Pattern pattern = Pattern.compile(regex);
        return collection.stream().anyMatch(s -> pattern.matcher(s).find());
    }

    // the stupid hamcrest matchers have compile erros depending whether they run on java6 or java7, so I rolled my own version
    // yes, I know this sucks... I want power asserts, as usual
    private void assertGraphiteMetricIsContained(final String id) {
        assertTrue(any(graphiteMockServer.getContent(), id));
    }

    private void assertGraphiteMetricIsNotContained(final String id) {
        assertFalse(any(graphiteMockServer.getContent(), id));
    }

    // Make sure no elements with a chars [] are included
    private void ensureValidKeyNames() {
        assertFalse(any(graphiteMockServer.getContent(), "\\.\\."));
        assertFalse(any(graphiteMockServer.getContent(), "\\["));
        assertFalse(any(graphiteMockServer.getContent(), "\\]"));
        assertFalse(any(graphiteMockServer.getContent(), "\\("));
        assertFalse(any(graphiteMockServer.getContent(), "\\)"));
    }

    private IndexResponse  indexElement(String index, String type, String fieldValue) {
        return client().prepareIndex(index, type).
                setSource("field", fieldValue)
                .execute().actionGet();
    }

    private SearchResponse  searchElement() {
        return client().prepareSearch(index).setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
    }
}
