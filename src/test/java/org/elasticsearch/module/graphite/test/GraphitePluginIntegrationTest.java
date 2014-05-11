package org.elasticsearch.module.graphite.test;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.collect.Iterables;
import org.elasticsearch.common.inject.ProvisionException;
import org.elasticsearch.node.Node;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;

import static org.elasticsearch.common.base.Predicates.containsPattern;
import static org.elasticsearch.module.graphite.test.NodeTestHelper.createNode;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class GraphitePluginIntegrationTest {

    public static final String GRAPHITE_SERVER_HOST = "localhost";
    public static final int GRAPHITE_SERVER_PORT = 12345;

    private GraphiteMockServer graphiteMockServer;

    private String clusterName = UUID.randomUUID().toString().replaceAll("-", "");
    private String index = UUID.randomUUID().toString().replaceAll("-", "");
    private String type = UUID.randomUUID().toString().replaceAll("-", "");
    private String defaultPrefix = "elasticsearch." + clusterName;
    private Node node;

    @Before
    public void startGraphiteMockServerAndNode() throws Exception {
        graphiteMockServer = new GraphiteMockServer(GRAPHITE_SERVER_PORT);
        graphiteMockServer.start();
    }

    @After
    public void stopGraphiteServer() throws Exception {
        graphiteMockServer.close();
        if (node != null && !node.isClosed()) {
            node.close();
        }
    }

    @Test
    public void testThatIndexingResultsInMonitoring() throws Exception {
        node = createNode(clusterName, true, GRAPHITE_SERVER_HOST, GRAPHITE_SERVER_PORT, "1s", null);
        IndexResponse indexResponse = indexElement(node, index, type, "value");
        assertThat(indexResponse.getId(), is(notNullValue()));

        Thread.sleep(2000);

        ensureValidKeyNames();
        assertGraphiteMetricIsContained(defaultPrefix + ".indexes." + index + ".id.0.indexing._all.indexCount 1");
        assertGraphiteMetricIsContained(defaultPrefix + ".indexes." + index + ".id.0.indexing." + type + ".indexCount 1");
        assertGraphiteMetricIsContained(defaultPrefix + ".node.jvm.threads.peakCount ");
    }


    @Test
    public void testThatFieldExclusionWorks() throws Exception {
        String excludeRegex = ".*\\.peakCount";
        node = createNode(clusterName, true, GRAPHITE_SERVER_HOST, GRAPHITE_SERVER_PORT, "1s", null, null, excludeRegex);

        IndexResponse indexResponse = indexElement(node, index, type, "value");
        assertThat(indexResponse.getId(), is(notNullValue()));

        Thread.sleep(2000);

        ensureValidKeyNames();
        assertGraphiteMetricIsNotContained(defaultPrefix + ".node.jvm.threads.peakCount ");
    }

    @Test
    public void testThatFieldInclusionWinsOverExclusion() throws Exception {
        String excludeRegex = ".*" + clusterName + ".*";
        String includeRegex = ".*\\.peakCount";
        node = createNode(clusterName, true, GRAPHITE_SERVER_HOST, GRAPHITE_SERVER_PORT, "1s", null, includeRegex, excludeRegex);

        IndexResponse indexResponse = indexElement(node, index, type, "value");
        assertThat(indexResponse.getId(), is(notNullValue()));

        Thread.sleep(2000);

        ensureValidKeyNames();
        assertGraphiteMetricIsNotContained(defaultPrefix + ".indexes." + index + ".id.0.indexing._all.indexCount 1");
        assertGraphiteMetricIsContained(defaultPrefix + ".node.jvm.threads.peakCount ");
    }

    @Test(expected = ProvisionException.class)
    public void testThatBrokenRegexLeadsToException() throws Exception {
        String excludeRegex = "*.peakCount";
        createNode(clusterName, true, GRAPHITE_SERVER_HOST, GRAPHITE_SERVER_PORT, "1s", null, null, excludeRegex);
    }

    @Test
    public void masterFailOverShouldWork() throws Exception {
        // Generate a new cluster not to interfer with the precedents tests
        String clusterName = UUID.randomUUID().toString().replaceAll("-", "");
        String defaultPrefix = "elasticsearch." + clusterName;

        node = createNode(clusterName, true, GRAPHITE_SERVER_HOST, GRAPHITE_SERVER_PORT, "1s", null);
        IndexResponse indexResponse = indexElement(node, index, type, "value");
        assertThat(indexResponse.getId(), is(notNullValue()));

        Node origNode = node;
        node = createNode(clusterName, true, GRAPHITE_SERVER_HOST, GRAPHITE_SERVER_PORT, "1s", null);
        graphiteMockServer.content.clear();
        origNode.stop();
        indexResponse = indexElement(node, index, type, "value");
        assertThat(indexResponse.getId(), is(notNullValue()));

        // wait for master fail over and writing to graph reporter
        Thread.sleep(2000);
        assertGraphiteMetricIsContained(defaultPrefix + ".indexes." + index + ".id.0.indexing._all.indexCount 1");
    }

    @Test
    public void testThatNotMasterNodeIsMonitoredAsWell() throws Exception {
        // Generate a new cluster not to interfer with the precedents tests
        String clusterName = UUID.randomUUID().toString().replaceAll("-", "");
        String defaultPrefix = "elasticsearch." + clusterName;

        // create a master node  without the plugin disabled
        Node masterNode = createNode(clusterName, true);
        graphiteMockServer.content.clear();
        Thread.sleep(2000);
        // create a non-master node with the plugin enabled, 
        // and a specific prefix to be sure that the metrics are emitted by that node
        String specificPrefix = defaultPrefix + ".dataNode";
        node = createNode(clusterName, false, GRAPHITE_SERVER_HOST, GRAPHITE_SERVER_PORT, "1s", specificPrefix);
        Thread.sleep(2000);

        assertGraphiteMetricIsContained(specificPrefix + ".node.jvm.uptime ");
    }

    @Test
    public void testThatNotMasterNodeIsNotMonitoredWhenNoMasterIsFound() throws Exception {
        // Generate a new cluster not to interfer with the precedents tests
        String clusterName = UUID.randomUUID().toString().replaceAll("-", "");
        String defaultPrefix = "elasticsearch." + clusterName;

        // create a non-master node with the plugin enabled, 
        // and no metric should be reported until a master is elected
        String specificPrefix = defaultPrefix + ".dataNode";
        graphiteMockServer.content.clear();
        node = createNode(clusterName, false, GRAPHITE_SERVER_HOST, GRAPHITE_SERVER_PORT, "1s", specificPrefix);
        Thread.sleep(4000);
        assertGraphiteMetricIsNotContained(specificPrefix + ".node.jvm.uptime ");

        // create a master node to be joigned...
        graphiteMockServer.content.clear();
        Node masterNode = createNode(clusterName, true);
        Thread.sleep(4000);
        //the dataNode should now start reporting metrics
        assertGraphiteMetricIsContained(specificPrefix + ".node.jvm.uptime ");
    }

    // the stupid hamcrest matchers have compile erros depending whether they run on java6 or java7, so I rolled my own version
    // yes, I know this sucks... I want power asserts, as usual
    private void assertGraphiteMetricIsContained(final String id) {
        assertThat(Iterables.any(graphiteMockServer.content, containsPattern(id)), is(true));
    }

    private void assertGraphiteMetricIsNotContained(final String id) {
        assertThat(Iterables.any(graphiteMockServer.content, containsPattern(id)), is(false));
    }

    // Make sure no elements with a chars [] are included
    private void ensureValidKeyNames() {
        assertThat(Iterables.any(graphiteMockServer.content, containsPattern("\\.\\.")), is(false));
        assertThat(Iterables.any(graphiteMockServer.content, containsPattern("\\[")), is(false));
        assertThat(Iterables.any(graphiteMockServer.content, containsPattern("\\]")), is(false));
        assertThat(Iterables.any(graphiteMockServer.content, containsPattern("\\(")), is(false));
        assertThat(Iterables.any(graphiteMockServer.content, containsPattern("\\)")), is(false));
    }

    private IndexResponse  indexElement(Node node, String index, String type, String fieldValue) {
        return node.client().prepareIndex(index, type).
                setSource("field", fieldValue)
                .execute().actionGet();
    }
}