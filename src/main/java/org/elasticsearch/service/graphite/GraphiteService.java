package org.elasticsearch.service.graphite;

import static java.util.Collections.emptyList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.NodeIndicesStats;
import org.elasticsearch.node.NodeService;

public class GraphiteService extends AbstractLifecycleComponent {

    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private NodeService nodeService;
    private final String graphiteHost;
    private final Integer graphitePort;
    private final TimeValue graphiteRefreshInternal;
    private final String graphitePrefix;
    private Pattern graphiteInclusionRegex;
    private Pattern graphiteExclusionRegex;
    private final Boolean perIndexMetrics;

    private volatile Thread graphiteReporterThread;
    private volatile boolean closed;
    
    public static Setting<TimeValue> EVERY_SETTING = Setting.timeSetting("metrics.graphite.every", TimeValue.timeValueMinutes(1), Property.Dynamic,Property.NodeScope);
    public static Setting<String> HOST_SETTING = Setting.simpleString("metrics.graphite.host", Property.NodeScope);
    public static Setting<Integer> PORT_SETTING = Setting.intSetting("metrics.graphite.port", 2003, Property.NodeScope);
    public static Setting<String> PREFIX = Setting.simpleString("metrics.graphite.prefix", Property.NodeScope);
    public static Setting<String> INCLUDE = Setting.simpleString("metrics.graphite.include", Property.NodeScope);
    public static Setting<String> EXCLUDE = Setting.simpleString("metrics.graphite.exclude", Property.NodeScope);
    public static Setting<Boolean> PER_INDEX = Setting.boolSetting("metrics.graphite.perIndex", false, Property.Dynamic, Property.NodeScope);
    public static Setting<List<String>> INCLUDE_INDEXES = Setting.listSetting("metrics.graphite.include.indexes", Arrays.asList(new String[]{"_all"}), Function.identity(), Property.NodeScope, Property.Dynamic);

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
        perIndexMetrics = settings.getAsBoolean("metrics.graphite.perIndex", false);
        String graphiteInclusionRegexString = settings.get("metrics.graphite.include");
        if (graphiteInclusionRegexString != null) {
            graphiteInclusionRegex = Pattern.compile(graphiteInclusionRegexString);
        }
        String graphiteExclusionRegexString = settings.get("metrics.graphite.exclude");
        if (graphiteExclusionRegexString != null) {
            graphiteExclusionRegex = Pattern.compile(graphiteExclusionRegexString);
        }
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        if (graphiteHost != null && graphiteHost.length() > 0) {
            graphiteReporterThread = EsExecutors.daemonThreadFactory(settings, "graphite_reporter").newThread(new GraphiteReporterThread(graphiteInclusionRegex, graphiteExclusionRegex));
            graphiteReporterThread.start();
            StringBuilder sb = new StringBuilder();
            if (graphiteInclusionRegex != null) sb.append("include [").append(graphiteInclusionRegex).append("] ");
            if (graphiteExclusionRegex != null) sb.append("exclude [").append(graphiteExclusionRegex).append("] ");
            logger.info("Graphite reporting triggered every [{}] to host [{}:{}] with metric prefix [{}] {}", graphiteRefreshInternal, graphiteHost, graphitePort, graphitePrefix, sb);
        } else {
            logger.error("Graphite reporting disabled, no graphite host configured");
        }
    }

    @Override
    protected void doStop() throws ElasticsearchException {
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
    protected void doClose() throws ElasticsearchException {}

    public class GraphiteReporterThread implements Runnable {

        private final Pattern graphiteInclusionRegex;
        private final Pattern graphiteExclusionRegex;

        public GraphiteReporterThread(Pattern graphiteInclusionRegex, Pattern graphiteExclusionRegex) {
            this.graphiteInclusionRegex = graphiteInclusionRegex;
            this.graphiteExclusionRegex = graphiteExclusionRegex;
        }

        public void run() {
            while (!closed) {
                boolean isClusterStarted = clusterService.lifecycleState().equals(Lifecycle.State.STARTED);
                if(isClusterStarted) {
                    DiscoveryNode node = clusterService.localNode();
                    if(node != null) {
                        long start = System.currentTimeMillis();
                        NodeIndicesStats nodeIndicesStats = indicesService.stats(false);
                        CommonStatsFlags commonStatsFlags = new CommonStatsFlags().clear();
                        NodeStats nodeStats = nodeService.stats(commonStatsFlags, true, true, true, true, true, true, true, true, true, true, true, true);
                        List<IndexShard> indexShards = null;
                        if(perIndexMetrics){
                            indexShards = getIndexShards(indicesService);
                        }

                        GraphiteReporter graphiteReporter = new GraphiteReporter(graphiteHost, graphitePort, graphitePrefix,
                                nodeIndicesStats, indexShards, nodeStats, graphiteInclusionRegex, graphiteExclusionRegex, node.getId(), start);
                        graphiteReporter.run();
                    } else {
                        if (node != null) {
                            logger.debug("[{}]/[{}] is not master node, not triggering update", node.getId(), node.getName());
                        }
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
            Set<String> includeIndexes = new HashSet<>(settings.getAsList(INCLUDE_INDEXES.getKey()));
            boolean all = includeIndexes.contains("_all");
            List<IndexShard> indexShards = new ArrayList<>();
            Iterator<IndexService> indexServiceIterator = indicesService.iterator();
            while (indexServiceIterator.hasNext()) {
                IndexService indexService = indexServiceIterator.next();
                String indexName = indexService.getMetaData().getIndex().getName();
                if(all || includeIndexes.contains(indexName)) {
                    for (int shardId : indexService.shardIds()) {
                        IndexShard shard = indexService.getShardOrNull(shardId);
                        indexShards.add(shard);
                    }
                }
            }
            return indexShards;
        }
    }
}
