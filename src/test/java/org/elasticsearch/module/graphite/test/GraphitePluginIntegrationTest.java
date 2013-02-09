package org.elasticsearch.module.graphite.test;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.RandomStringGenerator;
import org.elasticsearch.node.Node;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collection;

import static org.elasticsearch.module.graphite.test.NodeTestHelper.createNode;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class GraphitePluginIntegrationTest {

    public static final int GRAPHITE_SERVER_PORT = 12345;

    private GraphiteMockServer graphiteMockServer;

    private String clusterName = RandomStringGenerator.randomAlphabetic(10);
    private String index = RandomStringGenerator.randomAlphabetic(6).toLowerCase();
    private String type = RandomStringGenerator.randomAlphabetic(6).toLowerCase();
    private Node node;

    @Before
    public void startGraphiteMockServerAndNode() throws Exception {
        graphiteMockServer = new GraphiteMockServer(GRAPHITE_SERVER_PORT);
        graphiteMockServer.start();
        node = createNode(clusterName, 1, GRAPHITE_SERVER_PORT, "1s");
    }

    @After
    public void stopGraphiteServer() throws Exception {
        graphiteMockServer.close();
        if (!node.isClosed()) {
            node.close();
        }
    }

    @Test
    public void testThatIndexingResultsInMonitoring() throws Exception {
        IndexResponse indexResponse = indexElement(node, index, type, "value");
        assertThat(indexResponse.id(), is(notNullValue()));

        Thread.sleep(2000);

        ensureValidKeyNames();
        assertThat((Collection) graphiteMockServer.content, hasItem(startsWith("elasticsearch." + clusterName + ".indexes." + index + ".id.0.indexing._all.indexCount 1")));
        assertThat((Collection) graphiteMockServer.content, hasItem(startsWith("elasticsearch." + clusterName + ".indexes." + index + ".id.0.indexing." + type + ".indexCount 1")));
    }

    @Test
    public void masterFailOverShouldWork() throws Exception {
        String clusterName = RandomStringGenerator.randomAlphabetic(10);
        IndexResponse indexResponse = indexElement(node, index, type, "value");
        assertThat(indexResponse.id(), is(notNullValue()));

        Node origNode = node;
        node = createNode(clusterName, 1, GRAPHITE_SERVER_PORT, "1s");
        graphiteMockServer.content.clear();
        origNode.stop();
        indexResponse = indexElement(node, index, type, "value");
        assertThat(indexResponse.id(), is(notNullValue()));

        // wait for master fail over and writing to graph reporter
        Thread.sleep(2000);
        assertThat((Collection) graphiteMockServer.content, hasItem(startsWith("elasticsearch." + clusterName + ".indexes." + index + ".id.0.indexing._all.indexCount 1")));
    }

    private IndexResponse  indexElement(Node node, String index, String type, String fieldValue) {
        return node.client().prepareIndex(index, type).
                setSource("field", fieldValue)
                .execute().actionGet();
    }

    // Make sure no elements with a chars [] are included
    private void ensureValidKeyNames() {
        assertThat((Collection) graphiteMockServer.content, not(hasItem(containsString(".."))));
        assertThat((Collection) graphiteMockServer.content, not(hasItem(containsString("["))));
        assertThat((Collection) graphiteMockServer.content, not(hasItem(containsString("]"))));
        assertThat((Collection) graphiteMockServer.content, not(hasItem(containsString("("))));
        assertThat((Collection) graphiteMockServer.content, not(hasItem(containsString(")"))));
    }
}
