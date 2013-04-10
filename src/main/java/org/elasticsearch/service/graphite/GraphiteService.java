package org.elasticsearch.service.graphite;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.NodeIndicesStats;
import org.elasticsearch.node.service.NodeService;

import java.util.List;

public class GraphiteService extends AbstractLifecycleComponent<GraphiteService> {

    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private NodeService nodeService;
    private final String graphiteHost;
    private final Integer graphitePort;
    private final TimeValue graphiteRefreshInternal;
    private final String graphitePrefix;

    private volatile Thread graphiteReporterThread;
    private volatile boolean closed;

    @Inject public GraphiteService(Settings settings, ClusterService clusterService, IndicesService indicesService,
                                   NodeService nodeService) {
        super(settings);
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.nodeService = nodeService;
        graphiteRefreshInternal = settings.getAsTime("metrics.graphite.every", TimeValue.timeValueMinutes(1));
        graphiteHost = settings.get("metrics.graphite.host");
        graphitePort = settings.getAsInt("metrics.graphite.port", 2003);
        graphitePrefix = settings.get("metrics.graphite.prefix", "elasticsearch" + "." + settings.get("cluster.name"));
    }

    @Override
    protected void doStart() throws ElasticSearchException {
        if (graphiteHost != null && graphiteHost.length() > 0) {
            graphiteReporterThread = EsExecutors.daemonThreadFactory(settings, "graphite_reporter").newThread(new GraphiteReporterThread());
            graphiteReporterThread.start();
            logger.info("Graphite reporting triggered every [{}] to host [{}:{}] with metric prefix [{}]", graphiteRefreshInternal, graphiteHost, graphitePort, graphitePrefix);
        } else {
            logger.error("Graphite reporting disabled, no graphite host configured");
        }
    }

    @Override
    protected void doStop() throws ElasticSearchException {
        if (closed) {
            return;
        }
        if (graphiteReporterThread != null) {
            graphiteReporterThread.interrupt();
        }
        closed = true;
        logger.info("Graphite reporter stopped");
    }

    @Override
    protected void doClose() throws ElasticSearchException {}

    public class GraphiteReporterThread implements Runnable {

        public void run() {
            while (!closed) {
                DiscoveryNode node = clusterService.localNode();
                boolean isClusterStarted = clusterService.lifecycleState().equals(Lifecycle.State.STARTED);

                if (isClusterStarted && node != null && node.isMasterNode()) {
                    NodeIndicesStats nodeIndicesStats = indicesService.stats(false);
                    NodeStats nodeStats = nodeService.stats(false, true, true, true, true, true, true, true, true);
                    List<IndexShard> indexShards = getIndexShards(indicesService);

                    GraphiteReporter graphiteReporter = new GraphiteReporter(graphiteHost, graphitePort, graphitePrefix,
                            nodeIndicesStats, indexShards, nodeStats);
                    graphiteReporter.run();
                } else {
                    if (node != null) {
                        logger.debug("[{}]/[{}] is not master node, not triggering update", node.getId(), node.getName());
                    }
                }

                try {
                    Thread.sleep(graphiteRefreshInternal.millis());
                } catch (InterruptedException e1) {
                    continue;
                }
            }
        }

        private List<IndexShard> getIndexShards(IndicesService indicesService) {
            List<IndexShard> indexShards = Lists.newArrayList();
            String[] indices = indicesService.indices().toArray(new String[]{});
            for (String indexName : indices) {
                IndexService indexService = indicesService.indexServiceSafe(indexName);
                for (int shardId : indexService.shardIds()) {
                    indexShards.add(indexService.shard(shardId));
                }
            }
            return indexShards;
        }
    }
}
