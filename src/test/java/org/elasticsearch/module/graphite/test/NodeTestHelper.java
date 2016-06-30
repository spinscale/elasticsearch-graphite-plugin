package org.elasticsearch.module.graphite.test;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.log4j.LogConfigurator;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import java.io.IOException;

public class NodeTestHelper {

    public static Node createNode(String clusterName, int graphitePort, String refreshInterval) throws IOException {
        return createNode(clusterName, graphitePort, refreshInterval, null, null, null);
    }

    public static Node createNode(String clusterName, int graphitePort, String refreshInterval, String includeRegex,
                                  String excludeRegex, String prefix) throws IOException {
        Settings.Builder settingsBuilder = Settings.settingsBuilder();

        settingsBuilder.put("path.home", "target/es-data");
        settingsBuilder.put("path.conf", NodeTestHelper.class.getResource("/").getFile().replaceFirst("^/(.:/)", "$1"));
        settingsBuilder.put("path.plugins", "src/main/resources");

//        settingsBuilder.put("gateway.type", "none");
        settingsBuilder.put("cluster.name", clusterName);
        settingsBuilder.put("index.number_of_shards", 1);
        settingsBuilder.put("index.number_of_replicas", 1);

        settingsBuilder.put("metrics.graphite.host", "localhost");
        settingsBuilder.put("metrics.graphite.port", graphitePort);
        settingsBuilder.put("metrics.graphite.every", refreshInterval);
        if (!Strings.isEmpty(prefix)) {
            settingsBuilder.put("metrics.graphite.prefix", prefix);
        }

        if (Strings.hasLength(includeRegex)) {
            settingsBuilder.put("metrics.graphite.include", includeRegex);
        }

        if (Strings.hasLength(excludeRegex)) {
            settingsBuilder.put("metrics.graphite.exclude", excludeRegex);
        }

        LogConfigurator.configure(settingsBuilder.build(), true);

        return NodeBuilder.nodeBuilder().settings(settingsBuilder.build()).node();
    }
}
