package org.elasticsearch.service.graphite;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ClusterSettings;
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
    private static final Logger logger = LogManager.getLogger(GraphiteService.class);

    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private NodeService nodeService;
    private final String graphiteHost;
    private final Integer graphitePort;
    private TimeValue graphiteRefreshInternal;
    private final String graphitePrefix;
    private Pattern graphiteInclusionRegex;
    private Pattern graphiteExclusionRegex;
    private Boolean perIndexMetrics;
    private Set<String> includeIndices;
    private final Settings settings;

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
                                   NodeService nodeService, ClusterSettings clusterSettings) {
        super(settings);
        this.settings = settings;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.nodeService = nodeService;
        graphiteRefreshInternal = EVERY_SETTING.get(settings);
        graphiteHost = HOST_SETTING.get(settings);        
        graphitePort = PORT_SETTING.get(settings);
        
        //no good way to do this
        graphitePrefix = settings.get("metrics.graphite.prefix", "elasticsearch" + "." + settings.get("cluster.name"));
        perIndexMetrics = PER_INDEX.get(settings);
        String graphiteInclusionRegexString = INCLUDE.get(settings);
        if (graphiteInclusionRegexString != null) {
            graphiteInclusionRegex = Pattern.compile(graphiteInclusionRegexString);
        }
        String graphiteExclusionRegexString = EXCLUDE.get(settings);
        if (graphiteExclusionRegexString != null) {
            graphiteExclusionRegex = Pattern.compile(graphiteExclusionRegexString);
        }
        
        List<String> l = INCLUDE_INDEXES.get(settings);
        if(l != null) {
            includeIndices = new HashSet<>(l);
        }
        clusterSettings.addSettingsUpdateConsumer(INCLUDE_INDEXES, new Consumer<List<String>>() {
            public void accept(List<String> t) {
                Set<String> s = new HashSet<>();
                if(t != null) {
                    s.addAll(t);
                }
                includeIndices = s;
                logger.info("include indices {}", includeIndices);
            }
        });
        
        clusterSettings.addSettingsUpdateConsumer(PER_INDEX, new Consumer<Boolean>() {
            public void accept(Boolean b) {
                perIndexMetrics = b;
                logger.info("updating per index metric reporting");
            }
        });
        clusterSettings.addSettingsUpdateConsumer(EVERY_SETTING, new Consumer<TimeValue>() {
            public void accept(TimeValue t) {
                graphiteRefreshInternal = t;
                logger.info("updating graphite refresh interval new value {}", t);
            }
        });
    }
        
    
    @Override
    protected void doStart() throws ElasticsearchException {
        if (graphiteHost != null && graphiteHost.length() > 0) {
            graphiteReporterThread = EsExecutors.daemonThreadFactory(settings, "graphite_reporter").newThread(new GraphiteReporterThread());
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

        public void run() {
            while (!closed) {
                long start = System.currentTimeMillis();
                boolean isClusterStarted = clusterService.lifecycleState().equals(Lifecycle.State.STARTED);
                if(isClusterStarted) {
                    DiscoveryNode node = clusterService.localNode();
                    if(node != null) {
                        StopWatch sw = new StopWatch();
                        sw.start("indicesServiceStats");
                        NodeIndicesStats nodeIndicesStats = indicesService.stats(false);
                        sw.stop();
                        
                        sw.start("nodeServiceStats");
                        CommonStatsFlags commonStatsFlags = new CommonStatsFlags().clear();
                        NodeStats nodeStats = nodeService.stats(commonStatsFlags, true, true, true, true, true, true, true, true, true, true, true, true);
                        sw.stop();
                        
                        sw.start("indexShardsStats");
                        List<IndexShard> indexShards = null;
                        if(perIndexMetrics){
                            //Passing indices for a little more thread safety 
                            //since this can be updated via clusterSettings.
                            indexShards = getIndexShards(includeIndices, indicesService);
                        }
                        sw.stop();

                        StatsWriter statsWriter = new GraphiteStatsWriter(graphiteHost, graphitePort); 
                        GraphiteReporter graphiteReporter = new GraphiteReporter(statsWriter, graphitePrefix, nodeIndicesStats, 
                                indexShards, nodeStats, graphiteInclusionRegex, graphiteExclusionRegex, node.getId(), start, sw);
                        graphiteReporter.run();
                    } else {
                        logger.debug("Node is null.  No stats will be sent");
                    }
                }else {
                    logger.debug("ClusterService isn't started.  No stats will be sent");
                }
                try {
                    Thread.sleep(graphiteRefreshInternal.millis());
                } catch (InterruptedException ignored) {
                }
            }
        }
        
        private List<IndexShard> getIndexShards(Set<String> includes, IndicesService indicesService) {
            boolean all = includes.contains("_all");
            List<IndexShard> indexShards = new ArrayList<>();
            for (IndexService indexService : indicesService) {
                String indexName = indexService.getMetaData().getIndex().getName();
                if (all || includes.contains(indexName)) {
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
