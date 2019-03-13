package org.elasticsearch.service.graphite;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.common.FieldMemoryStats;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.StopWatch.TaskInfo;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.DiscoveryStats;
import org.elasticsearch.http.HttpStats;
import org.elasticsearch.index.cache.query.QueryCacheStats;
import org.elasticsearch.index.cache.request.RequestCacheStats;
import org.elasticsearch.index.engine.SegmentsStats;
import org.elasticsearch.index.fielddata.FieldDataStats;
import org.elasticsearch.index.flush.FlushStats;
import org.elasticsearch.index.get.GetStats;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.recovery.RecoveryStats;
import org.elasticsearch.index.refresh.RefreshStats;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.IndexingStats;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.index.translog.TranslogStats;
import org.elasticsearch.index.warmer.WarmerStats;
import org.elasticsearch.indices.NodeIndicesStats;
import org.elasticsearch.indices.breaker.AllCircuitBreakerStats;
import org.elasticsearch.indices.breaker.CircuitBreakerStats;
import org.elasticsearch.ingest.IngestStats;
import org.elasticsearch.monitor.fs.FsInfo;
import org.elasticsearch.monitor.fs.FsInfo.IoStats;
import org.elasticsearch.monitor.fs.FsInfo.Path;
import org.elasticsearch.monitor.jvm.JvmStats;
import org.elasticsearch.monitor.os.OsStats;
import org.elasticsearch.monitor.process.ProcessStats;
import org.elasticsearch.node.AdaptiveSelectionStats;
import org.elasticsearch.node.ResponseCollectorService.ComputedNodeStats;
import org.elasticsearch.script.ScriptStats;
import org.elasticsearch.search.suggest.completion.CompletionStats;
import org.elasticsearch.threadpool.ThreadPoolStats;
import org.elasticsearch.transport.TransportStats;

import com.carrotsearch.hppc.cursors.ObjectLongCursor;

public class GraphiteReporter {

    private static final Logger logger = LogManager.getLogger(GraphiteReporter.class);
    private final String prefix;    
    private List<IndexShard> indexShards;
    private NodeStats nodeStats;
    private final Pattern graphiteInclusionRegex;
    private final Pattern graphiteExclusionRegex;
    private final String timestamp;
    private final NodeIndicesStats nodeIndicesStats;
    private String id;
    private long start;
    private StatsWriter statsWriter = null;
    private StopWatch stopWatch;
    
    public GraphiteReporter(StatsWriter statsWriter, String prefix, NodeIndicesStats nodeIndicesStats, List<IndexShard> indexShards, NodeStats nodeStats,
            Pattern graphiteInclusionRegex, Pattern graphiteExclusionRegex, String id, long start, StopWatch stopWatch) {
        this.statsWriter = statsWriter;
        this.prefix = prefix;        
        this.indexShards = indexShards;
        this.nodeStats = nodeStats;
        this.graphiteInclusionRegex = graphiteInclusionRegex;
        this.graphiteExclusionRegex = graphiteExclusionRegex;
        this.timestamp = Long.toString(System.currentTimeMillis() / 1000);
        this.nodeIndicesStats = nodeIndicesStats;
        this.id = id;
        this.start = start;
        this.stopWatch = stopWatch;
    }

    public void run() {
        try {
            statsWriter.open();
            try{
                logger.debug("Sending nodeStats to graphite");
                sendNodeStats();
            }catch(Exception e){
                logException(e);
            }
            
            try{
                logger.debug("Sending nodeIndicesStats to graphite");
                sendNodeIndicesStats();
            }catch(Exception e){
                logException(e);
            }
            
            try{
                if(indexShards != null){
                    logger.debug("Sending indexShardStats size: {}", indexShards.size());
                    sendIndexShardStats();
                }else {
                    logger.debug("Skip sending indexShardStats indexShards == null");
                }
            }catch(Exception e){
                logException(e);
            }
            
            long took = System.currentTimeMillis() - start;
            sendInt(buildMetricName("plugin"), "totalTimeInMillis", took);
            for(TaskInfo info:stopWatch.taskInfo()) {
                sendInt(buildMetricName("plugin"), info.getTaskName() + "InMillis", info.getTime().getMillis());
            }
        } catch (Exception e) {
            logException(e);
        } finally {
            statsWriter.flushAndClose();
            statsWriter = null;
        }
    }

    private void sendNodeStats() {
        sendNodeFsStats(nodeStats.getFs());
        sendNodeHttpStats(nodeStats.getHttp());
        sendNodeJvmStats(nodeStats.getJvm());
        sendNodeOsStats(nodeStats.getOs());
        sendNodeProcessStats(nodeStats.getProcess());
        sendNodeTransportStats(nodeStats.getTransport());
        sendNodeThreadPoolStats(nodeStats.getThreadPool());
        sendNodeBreakerStats(nodeStats.getBreaker());
        
        sendAdaptiveSelectionStats(nodeStats.getAdaptiveSelectionStats());
        sendDiscoveryStats(nodeStats.getDiscoveryStats());
        sendIngestStats(nodeStats.getIngestStats());
        sendScriptStats(nodeStats.getScriptStats());
        
    }

    void sendScriptStats(ScriptStats scriptStats) {
        String type = buildMetricName("node.script");
        sendInt(type, "cacheEvictions", scriptStats.getCacheEvictions());
        sendInt(type, "compilations", scriptStats.getCompilations());
    }

    void sendIngestStats(IngestStats ingestStats) {
        sendIngestStats("total", ingestStats.getTotalStats());
        for (IngestStats.PipelineStat pipelineStats : ingestStats.getPipelineStats()) {
            sendIngestStats(pipelineStats.getPipelineId(), pipelineStats.getStats());
        }
    }

    void sendIngestStats(String pipeline, IngestStats.Stats stats) {
        if(stats != null) {
            String type = buildMetricName("node.ingest." + pipeline);        
            sendInt(type, "ingestCount", stats.getIngestCount());
            sendInt(type, "ingestFailedCount", stats.getIngestFailedCount());
            sendInt(type, "ingestTimeInMillis", stats.getIngestTimeInMillis());
            sendInt(type, "ingestCurrent", stats.getIngestCurrent());
        }
    }

    void sendAdaptiveSelectionStats(AdaptiveSelectionStats adaptiveSelectionStats) {
        String type = buildMetricName("node.selection");
        if(adaptiveSelectionStats != null && adaptiveSelectionStats.getComputedStats() != null) {
            ComputedNodeStats computedNodeStats = adaptiveSelectionStats.getComputedStats().get(this.id);
            if(computedNodeStats != null) {
                sendInt(type + ".computed", "queueSize", computedNodeStats.queueSize);
                sendFloat(type + ".computed", "responseTime", computedNodeStats.responseTime);
                sendFloat(type + ".computed", "serviceTime", computedNodeStats.serviceTime);
            }
        }
    }

    void sendDiscoveryStats(DiscoveryStats discoveryStats) {
        String type = buildMetricName("node.discovery.queue");
        sendFloat(type, "committed", discoveryStats.getQueueStats().getCommitted());
        sendFloat(type, "pending", discoveryStats.getQueueStats().getPending());
        sendFloat(type, "total", discoveryStats.getQueueStats().getTotal());
    }

    void sendNodeBreakerStats(AllCircuitBreakerStats breaker) {
        String type = buildMetricName("node.breaker");
        for (CircuitBreakerStats stats : breaker.getAllStats()) {
            String id = type + "." + stats.getName();
            sendInt(id, "estimated", stats.getEstimated());
            sendInt(id, "limit", stats.getLimit());
            sendInt(id, "trippedCount", stats.getTrippedCount());
            sendFloat(id, "overhead", stats.getOverhead());
        }
    }

    void sendNodeThreadPoolStats(ThreadPoolStats threadPoolStats) {
        String type = buildMetricName("node.threadpool");
        Iterator<ThreadPoolStats.Stats> statsIterator = threadPoolStats.iterator();
        while (statsIterator.hasNext()) {
            ThreadPoolStats.Stats stats = statsIterator.next();
            String id = type + "." + stats.getName();

            sendInt(id, "threads", stats.getThreads());
            sendInt(id, "queue", stats.getQueue());
            sendInt(id, "active", stats.getActive());
            sendInt(id, "rejected", stats.getRejected());
            sendInt(id, "largest", stats.getLargest());
            sendInt(id, "completed", stats.getCompleted());
        }
    }

    void sendNodeTransportStats(TransportStats transportStats) {
        String type = buildMetricName("node.transport");
        sendInt(type, "serverOpen", transportStats.serverOpen());
        sendInt(type, "rxCount", transportStats.rxCount());
        sendInt(type, "rxSizeBytes", transportStats.rxSize().getBytes());
        sendInt(type, "txCount", transportStats.txCount());
        sendInt(type, "txSizeBytes", transportStats.txSize().getBytes());
    }

    void sendNodeProcessStats(ProcessStats processStats) {
        String type = buildMetricName("node.process");

        sendInt(type, "openFileDescriptors", processStats.getOpenFileDescriptors());
        sendInt(type, "maxFileDescriptors", processStats.getMaxFileDescriptors());
        if (processStats.getCpu() != null) {
            sendInt(type + ".cpu", "percent", processStats.getCpu().getPercent());
            sendInt(type + ".cpu", "total_seconds", processStats.getCpu().getTotal().getSeconds());
        }

        if (processStats.getMem() != null) {
            sendInt(type + ".mem", "totalVirtual", processStats.getMem().getTotalVirtual().getBytes());
        }
        
    }

    void sendNodeOsStats(OsStats osStats) {
        String type = buildMetricName("node.os");

        if (osStats.getCpu() != null) {
            sendInt(type + ".cpu", "percent", osStats.getCpu().getPercent());
            
            double[] loadAverage = osStats.getCpu().getLoadAverage();
            if(loadAverage.length > 0) {
                sendFloat(type + ".cpu", "load_1_min", loadAverage[0]);
            }
            if(loadAverage.length > 1) {
                sendFloat(type + ".cpu", "load_5_min", loadAverage[1]);
            }
            if(loadAverage.length > 2) {
                sendFloat(type + ".cpu", "load_15_min", loadAverage[2]);
            }
            
        }
        
        if(osStats.getCgroup() != null) {
            sendInt(type + ".cgroup", "cpuAcctUsageNanos", osStats.getCgroup().getCpuAcctUsageNanos());
            sendInt(type + ".cgroup", "cpuCfsPeriodMicros", osStats.getCgroup().getCpuCfsPeriodMicros());
            sendInt(type + ".cgroup", "cpuCfsQuotaMicros", osStats.getCgroup().getCpuCfsQuotaMicros());
            sendBigInt(type + ".cgroup", "memoryLimitInBytes", osStats.getCgroup().getMemoryLimitInBytes());
            sendBigInt(type + ".cgroup", "memoryUsageInBytes", osStats.getCgroup().getMemoryUsageInBytes());
            
            if(osStats.getCgroup().getCpuStat() != null) {
                sendInt(type + ".cgroup.cpu", "numberOfElapsedPeriods", osStats.getCgroup().getCpuStat().getNumberOfElapsedPeriods());
                sendInt(type + ".cgroup.cpu", "numberOfTimesThrottled", osStats.getCgroup().getCpuStat().getNumberOfTimesThrottled());
                sendInt(type + ".cgroup.cpu", "timeThrottledNanos", osStats.getCgroup().getCpuStat().getTimeThrottledNanos());
            }
        }

        if (osStats.getMem() != null) {
            sendInt(type + ".mem", "freeBytes", osStats.getMem().getFree().getBytes());
            sendInt(type + ".mem", "usedBytes", osStats.getMem().getUsed().getBytes());
            sendInt(type + ".mem", "totalBytes", osStats.getMem().getTotal().getBytes());
            sendInt(type + ".mem", "freePercent", osStats.getMem().getFreePercent());
            sendInt(type + ".mem", "usedPercent", osStats.getMem().getUsedPercent());
        }

        if (osStats.getSwap() != null) {
            sendInt(type + ".swap", "freeBytes", osStats.getSwap().getFree().getBytes());
            sendInt(type + ".swap", "usedBytes", osStats.getSwap().getUsed().getBytes());
            sendInt(type + ".swap", "totalBytes", osStats.getSwap().getTotal().getBytes());
        }
        
    }
    

    void sendNodeJvmStats(JvmStats jvmStats) {
        String type = buildMetricName("node.jvm");
        sendSeconds(type, "uptime", jvmStats.getUptime());

        // mem
        if(jvmStats.getMem() != null) {
            sendInt(type + ".mem", "heapCommitted", jvmStats.getMem().getHeapCommitted().getBytes());
            sendInt(type + ".mem", "heapUsed", jvmStats.getMem().getHeapUsed().getBytes());
            sendInt(type + ".mem", "nonHeapCommitted", jvmStats.getMem().getNonHeapCommitted().getBytes());
            sendInt(type + ".mem", "nonHeapUsed", jvmStats.getMem().getNonHeapUsed().getBytes());
            
            Iterator<JvmStats.MemoryPool> memoryPoolIterator = jvmStats.getMem().iterator();
            while (memoryPoolIterator.hasNext()) {
                JvmStats.MemoryPool memoryPool = memoryPoolIterator.next();
                String memoryPoolType = type + ".mem.pool." + memoryPool.getName();

                sendInt(memoryPoolType, "max", memoryPool.getMax().getBytes());
                sendInt(memoryPoolType, "used", memoryPool.getUsed().getBytes());
                sendInt(memoryPoolType, "peakUsed", memoryPool.getPeakUsed().getBytes());
                sendInt(memoryPoolType, "peakMax", memoryPool.getPeakMax().getBytes());
            }            
        }


        // threads
        if(jvmStats.getThreads() != null) {
            sendInt(type + ".threads", "count", jvmStats.getThreads().getCount());
            sendInt(type + ".threads", "peakCount", jvmStats.getThreads().getPeakCount());
        }

        // garbage collectors
 
        if(jvmStats.getGc() != null) {
            for (JvmStats.GarbageCollector collector : jvmStats.getGc().getCollectors()) {
                String id = type + ".gc." + collector.getName();
                sendInt(id, "collectionCount", collector.getCollectionCount());
                sendInt(id, "collectionTimeSeconds", collector.getCollectionTime().seconds());
            }
        }
        
        // classes
        if(jvmStats.getClasses() != null) {
            sendInt(type + ".classes", "loadedClassCount", jvmStats.getClasses().getLoadedClassCount());
            sendInt(type + ".classes", "totalLoadedClassCount", jvmStats.getClasses().getTotalLoadedClassCount());
            sendInt(type + ".classes", "unloadedClassCount", jvmStats.getClasses().getUnloadedClassCount());
        }
        
        // TODO: bufferPools - where to get them?
    }

    void sendNodeHttpStats(HttpStats httpStats) {
        String type = buildMetricName("node.http");
        if(httpStats != null) {
            sendInt(type, "serverOpen", httpStats.getServerOpen());
            sendInt(type, "totalOpen", httpStats.getTotalOpen());
        }
    }

    void sendNodeFsStats(FsInfo fs) {
        
        sendNodeIoStats(fs.getIoStats());
        
        Iterator<Path> infoIterator = fs.iterator();
        int i = 0;
        while (infoIterator.hasNext()) {
            String type = buildMetricName("node.fs") + i;
            Path info = infoIterator.next();
            sendInt(type, "available", info.getAvailable().getBytes());
            sendInt(type, "total", info.getTotal().getBytes());
            sendInt(type, "free", info.getFree().getBytes());
            i++;
        }
    }

    void sendNodeIoStats(IoStats ioStats) {
        
        String type = buildMetricName("node.io");
        sendInt(type, "iops.total", ioStats.getTotalOperations());
        sendInt(type, "iops.read", ioStats.getTotalReadOperations());
        sendInt(type, "iops.write", ioStats.getTotalWriteOperations());        
        sendInt(type, "readInKiloBytes", ioStats.getTotalReadKilobytes());
        sendInt(type, "writeInKiloBytes", ioStats.getTotalWriteKilobytes());
    }

    void sendIndexShardStats() {
        for (IndexShard indexShard : indexShards) {
            String type = buildMetricName("indexes.") + indexShard.shardId().getIndex().getName() + ".id." + indexShard.shardId().id();
            sendIndexShardStats(type, indexShard);
        }
    }

    void sendIndexShardStats(String type, IndexShard indexShard) {
        
        sendRecoveryStats(type + ".store", indexShard.recoveryStats());        
        if(IndexShardState.STARTED.equals(indexShard.state())) {
            sendInt(type, "indexBufferRAMBytesUsed", indexShard.getIndexBufferRAMBytesUsed());
            sendInt(type, "activeOperationsCount", indexShard.getActiveOperationsCount());
            sendInt(type, "writingBytes", indexShard.getWritingBytes());
            sendInt(type, "shardBitsetFilterCacheSizeInBytes", indexShard.shardBitsetFilterCache().getMemorySizeInBytes());

            sendSearchStats(type + ".search", indexShard.searchStats("_all"));
            sendGetStats(type + ".get", indexShard.getStats());
            sendDocsStats(type + ".docs", indexShard.docStats());
            sendRefreshStats(type + ".refresh", indexShard.refreshStats());
            sendIndexingStats(type + ".indexing", indexShard.indexingStats("_all"));
            sendMergeStats(type + ".merge", indexShard.mergeStats());
            sendWarmerStats(type + ".warmer", indexShard.warmerStats());
            sendStoreStats(type + ".store", indexShard.storeStats());
            sendFlushStats(type + ".flush", indexShard.flushStats());
            sendRequestCacheStats(type + ".requestcache", indexShard.requestCache().stats());
            sendTranslogStats(type + ".translog", indexShard.translogStats());        
            sendFieldDataStats(type + ".fielddata", indexShard.fieldDataStats("*"));
        }
    }

    void sendStoreStats(String type, StoreStats storeStats) {
        if(storeStats != null) {
            sendInt(type, "sizeInBytes", storeStats.sizeInBytes());
        }
    }

    void sendWarmerStats(String type, WarmerStats warmerStats) {
        if(warmerStats != null) {
            sendInt(type, "current", warmerStats.current());
            sendInt(type, "total", warmerStats.total());
            sendInt(type, "totalTimeInMillis", warmerStats.totalTimeInMillis());
        }
    }

    void sendMergeStats(String type, MergeStats mergeStats) {
        if(mergeStats != null) {
            sendInt(type, "total", mergeStats.getTotal());
            sendInt(type, "totalTimeInMillis", mergeStats.getTotalTimeInMillis());
            sendInt(type, "totalNumDocs", mergeStats.getTotalNumDocs());
            sendInt(type, "current", mergeStats.getCurrent());
            sendInt(type, "currentNumDocs", mergeStats.getCurrentNumDocs());
            sendInt(type, "currentSizeInBytes", mergeStats.getCurrentSizeInBytes());
        }
    }

    void sendNodeIndicesStats() {
        String type = buildMetricName("node");
        sendDocsStats(type + ".docs", nodeIndicesStats.getDocs());
        sendFlushStats(type + ".flush", nodeIndicesStats.getFlush());
        sendGetStats(type + ".get", nodeIndicesStats.getGet());
        sendIndexingStats(type + ".indexing", nodeIndicesStats.getIndexing());
        sendRefreshStats(type + ".refresh", nodeIndicesStats.getRefresh());
        sendSearchStats(type + ".search", nodeIndicesStats.getSearch());
        sendFieldDataStats(type + ".fielddata", nodeIndicesStats.getFieldData());
        sendSegmentsStats(type + ".segments", nodeIndicesStats.getSegments());
        sendQueryCacheStats(type + ".querycache", nodeIndicesStats.getQueryCache());

        sendMergeStats(type + ".merge", nodeIndicesStats.getMerge());
        sendStoreStats(type + ".store", nodeIndicesStats.getStore());
        
        sendCompletionStats(type+ ".completion", nodeIndicesStats.getCompletion());
        sendRecoveryStats(type+ ".recovery", nodeIndicesStats.getRecoveryStats());
        sendRequestCacheStats(type+ ".requestCache", nodeIndicesStats.getRequestCache());
        sendTranslogStats(type+ ".translog", nodeIndicesStats.getTranslog());
        sendNodeWarmerStats(type+ ".warmer", nodeIndicesStats.getWarmer());
        
    }

    void sendNodeWarmerStats(String type, WarmerStats warmer) {
        sendInt(type, "current", warmer.current());
        sendInt(type, "total", warmer.total());
        sendInt(type, "totalTimeInMillis", warmer.totalTimeInMillis());
    }

    void sendTranslogStats(String type, TranslogStats translog) {
        if(translog != null) {
            sendInt(type, "sizeInBytes", translog.getTranslogSizeInBytes());
            sendInt(type, "uncommittedSizeInBytes", translog.getUncommittedSizeInBytes());
            sendInt(type, "estimatedNumberOfOperations", translog.estimatedNumberOfOperations());
            sendInt(type, "uncommittedOperations", translog.getUncommittedOperations());
        }
    }

    void sendRequestCacheStats(String type, RequestCacheStats requestCache) {
        sendInt(type, "evictions", requestCache.getEvictions());
        sendInt(type, "hitCount", requestCache.getHitCount());
        sendInt(type, "memorySizeInBytes", requestCache.getMemorySizeInBytes());
        sendInt(type, "missCount", requestCache.getMissCount());
    }

    void sendRecoveryStats(String type, RecoveryStats recoveryStats) {
        if(recoveryStats != null) {
            sendInt(type, "throttleTimeInMillis", recoveryStats.throttleTime().millis());
            sendInt(type, "currentAsSource", recoveryStats.currentAsSource());
            sendInt(type, "currentAsTarget", recoveryStats.currentAsTarget());
        }
    }

    void sendCompletionStats(String type, CompletionStats completion) {
        sendInt(type, "sizeInBytes", completion.getSizeInBytes());
        FieldMemoryStats fields = completion.getFields();
        if(fields != null) {
            Iterator<ObjectLongCursor<String>> i = fields.iterator();
            while(i.hasNext()) {
                ObjectLongCursor<String> item = i.next();
                sendInt(type + ".fields", item.key, item.value);
            }
        }
    }

    void sendQueryCacheStats(String type, QueryCacheStats queryCache) {
        sendInt(type, "evictions", queryCache.getEvictions());
        sendInt(type, "hitCount", queryCache.getHitCount());
        sendInt(type, "memorySizeInBytes", queryCache.getMemorySizeInBytes());
        sendInt(type, "missCount", queryCache.getMissCount());
    }

    void sendSegmentsStats(String type, SegmentsStats segments) {
        sendInt(type, "count", segments.getCount());
        
        sendInt(type, "docValuesMemory", segments.getDocValuesMemoryInBytes());
        sendInt(type, "bitSetMemoryInBytes", segments.getBitsetMemoryInBytes());
        sendInt(type, "indexWriterMemoryInBytes", segments.getIndexWriterMemoryInBytes());
        sendInt(type, "memoryInBytes", segments.getMemoryInBytes());
        sendInt(type, "versionMapMemoryInBytes", segments.getVersionMapMemoryInBytes());
        
        sendInt(type, "normsMemoryInBytes", segments.getNormsMemoryInBytes());
        sendInt(type, "pointsMemoryInBytes", segments.getPointsMemoryInBytes());
        sendInt(type, "storedFieldsMemoryInBytes", segments.getStoredFieldsMemoryInBytes());
        sendInt(type, "termsMemoryInBytes", segments.getTermsMemoryInBytes());
        sendInt(type, "termVectorsMemoryInBytes", segments.getTermVectorsMemoryInBytes());
        
    }

    void sendSearchStats(String type, SearchStats searchStats) {
        if(searchStats != null) {
            SearchStats.Stats totalSearchStats = searchStats.getTotal();
            sendInt(type, "openContexts", searchStats.getOpenContexts());
            sendSearchStatsStats(type + "._all", totalSearchStats);
            

            if (searchStats.getGroupStats() != null ) {
                for (Map.Entry<String, SearchStats.Stats> statsEntry : searchStats.getGroupStats().entrySet()) {
                    sendSearchStatsStats(type + "." + statsEntry.getKey(), statsEntry.getValue());
                }
            }
        }
    }

    void sendSearchStatsStats(String type, SearchStats.Stats searchStats) {
        sendInt(type, "queryCount", searchStats.getQueryCount());
        sendInt(type, "queryTimeInMillis", searchStats.getQueryTimeInMillis());
        sendInt(type, "queryCurrent", searchStats.getQueryCurrent());
        
        sendInt(type, "fetchCount", searchStats.getFetchCount());
        sendInt(type, "fetchTimeInMillis", searchStats.getFetchTimeInMillis());
        sendInt(type, "fetchCurrent", searchStats.getFetchCurrent());
        
        sendInt(type, "scrollCount", searchStats.getScrollCount());
        sendInt(type, "scrollTimeInMillis", searchStats.getScrollTimeInMillis());
        sendInt(type, "scrollCurrent", searchStats.getScrollCurrent());
        
        sendInt(type, "suggestCount", searchStats.getSuggestCount());
        sendInt(type, "suggestTimeInMillis", searchStats.getSuggestTimeInMillis());
        sendInt(type, "suggestCurrent", searchStats.getSuggestCurrent());
    }

    void sendRefreshStats(String type, RefreshStats refreshStats) {
        if(refreshStats != null){
            sendInt(type, "total", refreshStats.getTotal());
            sendInt(type, "totalTimeInMillis", refreshStats.getTotalTimeInMillis());
            sendInt(type, "waitingListeners", refreshStats.getListeners());
        }
    }

    void sendIndexingStats(String type, IndexingStats indexingStats) {
        if(indexingStats != null) {
            IndexingStats.Stats totalStats = indexingStats.getTotal();
            sendStats(type + "._all", totalStats);

            Map<String, IndexingStats.Stats> typeStats = indexingStats.getTypeStats();
            if (typeStats != null) {
                for (Map.Entry<String, IndexingStats.Stats> statsEntry : typeStats.entrySet()) {
                    sendStats(type + "." + statsEntry.getKey(), statsEntry.getValue());
                }
            }
        }
    }

    void sendStats(String type, IndexingStats.Stats stats) {
        sendInt(type, "indexCount", stats.getIndexCount());
        sendInt(type, "indexTimeInMillis", stats.getIndexTime().millis());
        sendInt(type, "indexCurrent", stats.getIndexCount());
        sendInt(type, "deleteCount", stats.getDeleteCount());
        sendInt(type, "deleteTimeInMillis", stats.getDeleteTime().millis());
        sendInt(type, "deleteCurrent", stats.getDeleteCurrent());
        sendInt(type, "noopUpdateCount", stats.getNoopUpdateCount());
        sendInt(type, "throttleTimeInMillis", stats.getThrottleTime().getMillis());
        
    }

    void sendGetStats(String type, GetStats getStats) {
        if(getStats != null) {
            sendInt(type, "existsCount", getStats.getExistsCount());
            sendInt(type, "existsTimeInMillis", getStats.getExistsTimeInMillis());
            sendInt(type, "missingCount", getStats.getMissingCount());
            sendInt(type, "missingTimeInMillis", getStats.getMissingTimeInMillis());
            sendInt(type, "current", getStats.current());
            sendInt(type, "getCount", getStats.getCount());
            sendInt(type, "getTimeInMillis", getStats.getTimeInMillis());
        }
    }

    void sendFlushStats(String type, FlushStats flush) {
        if(flush != null) {
            sendInt(type, "total", flush.getTotal());
            sendInt(type, "totalTimeInMillis", flush.getTotalTimeInMillis());
        }
    }

    void sendDocsStats(String name, DocsStats docsStats) {
        if(docsStats != null) {
            sendInt(name, "count", docsStats.getCount());
            sendInt(name, "deleted", docsStats.getDeleted());
            sendInt(name, "averageSizeInBytes", docsStats.getAverageSizeInBytes());
            sendInt(name, "totalSizeInBytes", docsStats.getTotalSizeInBytes());
        }
    }

    void sendFieldDataStats(String name, FieldDataStats fieldDataStats) {
        if(fieldDataStats != null) {
            sendInt(name, "memorySizeInBytes", fieldDataStats.getMemorySizeInBytes());
            sendInt(name, "evictions", fieldDataStats.getEvictions());
            
            FieldMemoryStats fields = fieldDataStats.getFields();
            if(fields != null) {
                Iterator<ObjectLongCursor<String>> i = fields.iterator();
                while(i.hasNext()) {
                    ObjectLongCursor<String> item = i.next();
                    sendInt(name, "fields." + item.key, item.value);
                }
            }
        }
    }

    protected void sendToGraphite(String name, String value) {
        try {
            String nameToSend = sanitizeString(name);
            // check if this value is excluded
            if (graphiteExclusionRegex != null && graphiteExclusionRegex.matcher(nameToSend).matches()) {
                if (graphiteInclusionRegex == null ||
                    (graphiteInclusionRegex != null && !graphiteInclusionRegex.matcher(nameToSend).matches())) {
                    return;
                }
            }
            statsWriter.write(nameToSend);
            statsWriter.write(' ');
            statsWriter.write(value);
            statsWriter.write(' ');
            statsWriter.write(timestamp);
            statsWriter.write('\n');
            statsWriter.flush();
        } catch (IOException e) {
            logger.info("Error sending to Graphite:", e);
        }
    }

    private void sendSeconds(String type, String valueName, TimeValue value) {
        if(value != null) {
            sendInt(type, valueName, value.seconds());
        }
    }

    protected void sendInt(String name, String valueName, ByteSizeValue value) {
        if(value != null) {
            sendInt(name, valueName, value.getBytes());
        }
    }

    protected void sendInt(String name, String valueName, long value) {
        sendToGraphite(name + "." + valueName, String.format("%d", value));
    }

    protected void sendBigInt(String name, String valueName, String value) {
        if(value != null) {
            sendToGraphite(name + "." + valueName, value);
        }
    }

    protected void sendFloat(String name, String valueName, double value) {
        sendToGraphite(name + "." + valueName, String.format("%2.2f", value));
    }

    protected String sanitizeString(String s) {
        return s.replace(' ', '-');
    }

    protected String buildMetricName(String name) {
        return prefix + "." + name;
    }

    private void logException(Exception e) {
        logger.info("Error writing to Graphite", e);
    }
}
