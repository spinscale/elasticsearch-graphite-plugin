package org.elasticsearch.module.graphite.test;

import java.io.IOException;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.node.Node;

public class NodeTestHelper {

    public static Node createNode(String clusterName, int graphitePort, String refreshInterval) throws IOException {
        return createNode(clusterName, graphitePort, refreshInterval, null, null, null);
    }

    public static Node createNode(String clusterName, int graphitePort, String refreshInterval, String includeRegex,
                                  String excludeRegex, String prefix) throws IOException {
        Builder settingsBuilder = Settings.builder();

  //      settingsBuilder.put("path.conf", NodeTestHelper.class.getResource("/").getFile());
        settingsBuilder.put("path.home", NodeTestHelper.class.getResource("/").getFile());

//        settingsBuilder.put("gateway.type", "none");
        settingsBuilder.put("cluster.name", clusterName);
//        settingsBuilder.put("index.number_of_shards", 1);
//        settingsBuilder.put("index.number_of_replicas", 1);

//        settingsBuilder.put("metrics.graphite.perIndex", true);
//        settingsBuilder.put("metrics.graphite.host", "localhost");
//        settingsBuilder.put("metrics.graphite.port", graphitePort);
//        settingsBuilder.put("metrics.graphite.every", refreshInterval);
//        if (!Strings.isEmpty(prefix)) {
//            settingsBuilder.put("metrics.graphite.prefix", prefix);
//        }
//
//        if (Strings.hasLength(includeRegex)) {
//            settingsBuilder.put("metrics.graphite.include", includeRegex);
//        }
//
//        if (Strings.hasLength(excludeRegex)) {
//            settingsBuilder.put("metrics.graphite.exclude", excludeRegex);
//        }

        LogConfigurator.configureWithoutConfig(settingsBuilder.build());

        return new Node(settingsBuilder.build());
    }
}
