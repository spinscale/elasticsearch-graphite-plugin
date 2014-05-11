package org.elasticsearch.module.graphite.test;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.log4j.LogConfigurator;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import java.io.IOException;

public class NodeTestHelper {

    public static Node createNode(String clusterName, boolean nodeMaster) throws IOException {
        return createNode(clusterName, nodeMaster, null, 0, null, null, null, null);
    }

    public static Node createNode(String clusterName, boolean nodeMaster, String graphiteHost, int graphitePort, String refreshInterval, String graphitePrefix) throws IOException {
        return createNode(clusterName, nodeMaster, graphiteHost, graphitePort, refreshInterval, graphitePrefix, null, null);
    }

    public static Node createNode(String clusterName, boolean nodeMaster, String graphiteHost, int graphitePort, String refreshInterval, String graphitePrefix, String includeRegex,
                                  String excludeRegex) throws IOException {
        ImmutableSettings.Builder settingsBuilder = ImmutableSettings.settingsBuilder();

        settingsBuilder.put("gateway.type", "none");
        settingsBuilder.put("cluster.name", clusterName);
        settingsBuilder.put("node.master", nodeMaster);
        settingsBuilder.put("index.number_of_shards", 1);
        settingsBuilder.put("index.number_of_replicas", 1);
        settingsBuilder.put("discovery.zen.ping.multicast.enabled", false);
        settingsBuilder.put("discovery.zen.ping.unicast.hosts",  "localhost:9300,localhost:9301,localhost:9302");
        
        if (Strings.hasLength(graphiteHost)) {
          settingsBuilder.put("metrics.graphite.host", graphiteHost);
          settingsBuilder.put("metrics.graphite.port", graphitePort);
          settingsBuilder.put("metrics.graphite.every", refreshInterval);
            if (Strings.hasLength(graphitePrefix)) {
                settingsBuilder.put("metrics.graphite.prefix", graphitePrefix);
            }
        }

        if (Strings.hasLength(includeRegex)) {
            settingsBuilder.put("metrics.graphite.include", includeRegex);
        }

        if (Strings.hasLength(excludeRegex)) {
            settingsBuilder.put("metrics.graphite.exclude", excludeRegex);
        }

        LogConfigurator.configure(settingsBuilder.build());

        return NodeBuilder.nodeBuilder().settings(settingsBuilder.build()).node();
    }
}