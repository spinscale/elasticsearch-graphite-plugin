package org.elasticsearch.module.graphite.test;

import org.elasticsearch.common.RandomStringGenerator;
import org.elasticsearch.common.logging.log4j.LogConfigurator;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import java.io.IOException;

public class NodeTestHelper {

    public static Node createNode(String clusterName, final int numberOfShards, int graphitePort, String refreshInterval) throws IOException {
        ImmutableSettings.Builder settingsBuilder = ImmutableSettings.settingsBuilder();

        settingsBuilder.put("gateway.type", "none");
        settingsBuilder.put("cluster.name", clusterName);
        settingsBuilder.put("index.number_of_shards", numberOfShards);
        settingsBuilder.put("index.number_of_replicas", 1);

        settingsBuilder.put("metrics.graphite.host", "localhost");
        settingsBuilder.put("metrics.graphite.port", graphitePort);
        settingsBuilder.put("metrics.graphite.every", refreshInterval);

        LogConfigurator.configure(settingsBuilder.build());

        return NodeBuilder.nodeBuilder().settings(settingsBuilder.build()).node();
    }
}
