package org.elasticsearch.service.graphite;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.math.BigInteger;
import java.net.Socket;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.discovery.DiscoveryStats;
import org.elasticsearch.http.HttpStats;
import org.elasticsearch.index.cache.query.QueryCacheStats;
import org.elasticsearch.index.engine.SegmentsStats;
import org.elasticsearch.index.fielddata.FieldDataStats;
import org.elasticsearch.index.flush.FlushStats;
import org.elasticsearch.index.get.GetStats;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.refresh.RefreshStats;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexingStats;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.index.warmer.WarmerStats;
import org.elasticsearch.indices.NodeIndicesStats;
import org.elasticsearch.indices.breaker.AllCircuitBreakerStats;
import org.elasticsearch.indices.breaker.CircuitBreakerStats;
import org.elasticsearch.ingest.IngestStats;
import org.elasticsearch.monitor.fs.FsInfo;
import org.elasticsearch.monitor.fs.FsInfo.Path;
import org.elasticsearch.monitor.jvm.JvmStats;
import org.elasticsearch.monitor.os.OsStats;
import org.elasticsearch.monitor.process.ProcessStats;
import org.elasticsearch.node.AdaptiveSelectionStats;
import org.elasticsearch.node.ResponseCollectorService.ComputedNodeStats;
import org.elasticsearch.script.ScriptStats;
import org.elasticsearch.threadpool.ThreadPoolStats;
import org.elasticsearch.transport.TransportStats;

public class GraphiteReporter {

    private static final Logger logger = ESLoggerFactory.getLogger(GraphiteReporter.class.getName());
    private Writer writer;
    private final String host;
    private final int port;
    private final String prefix;    
    private List<IndexShard> indexShards;
    private NodeStats nodeStats;
    private final Pattern graphiteInclusionRegex;
    private final Pattern graphiteExclusionRegex;
    private final String timestamp;
    private final NodeIndicesStats nodeIndicesStats;
    private String id;
    private long start;


    public GraphiteReporter(String host, int port, String prefix, NodeIndicesStats nodeIndicesStats,
                            List<IndexShard> indexShards, NodeStats nodeStats,
                            Pattern graphiteInclusionRegex, Pattern graphiteExclusionRegex, String id, long start) {
        this.host = host;
        this.port = port;
        this.prefix = prefix;        
        this.indexShards = indexShards;
        this.nodeStats = nodeStats;
        this.graphiteInclusionRegex = graphiteInclusionRegex;
        this.graphiteExclusionRegex = graphiteExclusionRegex;
        this.timestamp = Long.toString(System.currentTimeMillis() / 1000);
        this.nodeIndicesStats = nodeIndicesStats;
        this.id = id;
        this.start = start;
    }

    public void run() {
        Socket socket = null;
        try {
            socket = getSocket();
            writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));

            try{
                sendNodeStats();
            }catch(Exception e){
                logException(e);
            }
            
            try{
                sendNodeIndicesStats();
            }catch(Exception e){
                logException(e);
            }
            
            try{
                if(indexShards != null){
                    sendIndexShardStats();
                }
            }catch(Exception e){
                logException(e);
            }
            
            long took = System.currentTimeMillis() - start;
            sendInt(buildMetricName("plugin"), "ingestTimeInMillis", took);
        } catch (Exception e) {
            logException(e);
        } finally {
            flushWriter();
            closeSocket(socket);
            writer = null;
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

    private void sendScriptStats(ScriptStats scriptStats) {
        String type = buildMetricName("node.script");
        sendInt(type, "cacheEvictions", scriptStats.getCacheEvictions());
        sendInt(type, "compilations", scriptStats.getCompilations());
    }

    private void sendIngestStats(IngestStats ingestStats) {
        String type = buildMetricName("node.ingest");
        sendIngestStats("total", ingestStats.getTotalStats());
        for(String pipeline: ingestStats.getStatsPerPipeline().keySet()) {
            sendIngestStats(pipeline, ingestStats.getStatsPerPipeline().get(pipeline));
        }
    }

    private void sendIngestStats(String pipeline, IngestStats.Stats stats) {
        String type = buildMetricName("node.ingest." + pipeline);
        sendInt(type, "ingestCount", stats.getIngestCount());
        sendInt(type, "ingestFailedCount", stats.getIngestFailedCount());
        sendInt(type, "ingestTimeInMillis", stats.getIngestTimeInMillis());
        sendInt(type, "ingestCurrent", stats.getIngestCurrent());
    }

    private void sendAdaptiveSelectionStats(AdaptiveSelectionStats adaptiveSelectionStats) {
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

    private void sendDiscoveryStats(DiscoveryStats discoveryStats) {
        String type = buildMetricName("node.discovery.queue");
        sendFloat(type, "committed", discoveryStats.getQueueStats().getCommitted());
        sendFloat(type, "pending", discoveryStats.getQueueStats().getPending());
        sendFloat(type, "total", discoveryStats.getQueueStats().getTotal());
    }

    private void sendNodeBreakerStats(AllCircuitBreakerStats breaker) {
        String type = buildMetricName("node.breaker");
        for (CircuitBreakerStats stats : breaker.getAllStats()) {
            String id = type + "." + stats.getName();
            sendInt(id, "estimated", stats.getEstimated());
            sendInt(id, "limit", stats.getLimit());
            sendInt(id, "trippedCount", stats.getTrippedCount());
            sendFloat(id, "overhead", stats.getOverhead());
        }
    }

    private void sendNodeThreadPoolStats(ThreadPoolStats threadPoolStats) {
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

    private void sendNodeTransportStats(TransportStats transportStats) {
        String type = buildMetricName("node.transport");
        sendInt(type, "serverOpen", transportStats.serverOpen());
        sendInt(type, "rxCount", transportStats.rxCount());
        sendInt(type, "rxSizeBytes", transportStats.rxSize().getBytes());
        sendInt(type, "txCount", transportStats.txCount());
        sendInt(type, "txSizeBytes", transportStats.txSize().getBytes());
    }

    private void sendNodeProcessStats(ProcessStats processStats) {
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

    private void sendNodeOsStats(OsStats osStats) {
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
            sendInt(type + ".cgroup", "memoryLimitInBytes", new BigInteger(osStats.getCgroup().getMemoryLimitInBytes()));
            sendInt(type + ".cgroup", "memoryUsageInBytes", new BigInteger(osStats.getCgroup().getMemoryUsageInBytes()));
            
            if(osStats.getCgroup().getCpuStat() != null) {
                sendInt(type + ".cgroup.cpu", "numberOfElapsedPeriods", osStats.getCgroup().getCpuStat().getNumberOfElapsedPeriods());
                sendInt(type + ".cgroup.cpu", "numberOfTimesThrottled", osStats.getCgroup().getCpuStat().getNumberOfTimesThrottled());
                sendInt(type + ".cgroup.cpu", "timeThrottledNanos", osStats.getCgroup().getCpuStat().getTimeThrottledNanos());
            }
        }

        if (osStats.getMem() != null) {
            sendInt(type + ".mem", "freeBytes", osStats.getMem().getFree().getBytes());
            sendInt(type + ".mem", "usedBytes", osStats.getMem().getUsed().getBytes());
            sendInt(type + ".mem", "freePercent", osStats.getMem().getFreePercent());
            sendInt(type + ".mem", "usedPercent", osStats.getMem().getUsedPercent());
        }

        if (osStats.getSwap() != null) {
            sendInt(type + ".swap", "freeBytes", osStats.getSwap().getFree().getBytes());
            sendInt(type + ".swap", "usedBytes", osStats.getSwap().getUsed().getBytes());
            sendInt(type + ".swap", "totalBytes", osStats.getSwap().getTotal().getBytes());
        }
        
    }
    

    private void sendNodeJvmStats(JvmStats jvmStats) {
        String type = buildMetricName("node.jvm");
        sendInt(type, "uptime", jvmStats.getUptime().seconds());

        // mem
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

        // threads
        sendInt(type + ".threads", "count", jvmStats.getThreads().getCount());
        sendInt(type + ".threads", "peakCount", jvmStats.getThreads().getPeakCount());

        // garbage collectors
 
        for (JvmStats.GarbageCollector collector : jvmStats.getGc().getCollectors()) {
            String id = type + ".gc." + collector.getName();
            sendInt(id, "collectionCount", collector.getCollectionCount());
            sendInt(id, "collectionTimeSeconds", collector.getCollectionTime().seconds());
        }
        
        // classes
        sendInt(type + ".classes", "loadedClassCount", jvmStats.getClasses().getLoadedClassCount());
        sendInt(type + ".classes", "totalLoadedClassCount", jvmStats.getClasses().getTotalLoadedClassCount());
        sendInt(type + ".classes", "unloadedClassCount", jvmStats.getClasses().getUnloadedClassCount());
        
        // TODO: bufferPools - where to get them?
    }

    private void sendNodeHttpStats(HttpStats httpStats) {
        String type = buildMetricName("node.http");
        if(httpStats != null) {
            sendInt(type, "serverOpen", httpStats.getServerOpen());
            sendInt(type, "totalOpen", httpStats.getTotalOpen());
        }
    }

    private void sendNodeFsStats(FsInfo fs) {
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

    private void sendIndexShardStats() {
        for (IndexShard indexShard : indexShards) {
            String type = buildMetricName("indexes.") + indexShard.shardId().getIndex().getName() + ".id." + indexShard.shardId().id();
            sendIndexShardStats(type, indexShard);
        }
    }

    private void sendIndexShardStats(String type, IndexShard indexShard) {        
        sendSearchStats(type + ".search", indexShard.searchStats());
        sendGetStats(type + ".get", indexShard.getStats());
        sendDocsStats(type + ".docs", indexShard.docStats());
        sendRefreshStats(type + ".refresh", indexShard.refreshStats());
        sendIndexingStats(type + ".indexing", indexShard.indexingStats("_all"));
        sendMergeStats(type + ".merge", indexShard.mergeStats());
        sendWarmerStats(type + ".warmer", indexShard.warmerStats());
        sendStoreStats(type + ".store", indexShard.storeStats());
    }

    private void sendStoreStats(String type, StoreStats storeStats) {
        sendInt(type, "sizeInBytes", storeStats.sizeInBytes());
    }

    private void sendWarmerStats(String type, WarmerStats warmerStats) {
        sendInt(type, "current", warmerStats.current());
        sendInt(type, "total", warmerStats.total());
        sendInt(type, "totalTimeInMillis", warmerStats.totalTimeInMillis());
    }

    private void sendMergeStats(String type, MergeStats mergeStats) {
        sendInt(type, "total", mergeStats.getTotal());
        sendInt(type, "totalTimeInMillis", mergeStats.getTotalTimeInMillis());
        sendInt(type, "totalNumDocs", mergeStats.getTotalNumDocs());
        sendInt(type, "current", mergeStats.getCurrent());
        sendInt(type, "currentNumDocs", mergeStats.getCurrentNumDocs());
        sendInt(type, "currentSizeInBytes", mergeStats.getCurrentSizeInBytes());
    }

    private void sendNodeIndicesStats() {
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
        
        //TODO.  Not needed now and not trivial.
//        sendCompletionStats(type + ".completion", nodeIndicesStats.getCompletion());
        
    }

    private void sendQueryCacheStats(String type, QueryCacheStats queryCache) {
        sendInt(type, "evictions", queryCache.getEvictions());
        sendInt(type, "hitCount", queryCache.getHitCount());
        sendInt(type, "memorySizeInBytes", queryCache.getMemorySizeInBytes());
        sendInt(type, "missCount", queryCache.getMissCount());
    }

    private void sendSegmentsStats(String type, SegmentsStats segments) {
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

    private void sendSearchStats(String type, SearchStats searchStats) {
        SearchStats.Stats totalSearchStats = searchStats.getTotal();
        sendInt(type, "openContexts", searchStats.getOpenContexts());
        sendSearchStatsStats(type + "._all", totalSearchStats);
        

        if (searchStats.getGroupStats() != null ) {
            for (Map.Entry<String, SearchStats.Stats> statsEntry : searchStats.getGroupStats().entrySet()) {
                sendSearchStatsStats(type + "." + statsEntry.getKey(), statsEntry.getValue());
            }
        }
    }

    private void sendSearchStatsStats(String type, SearchStats.Stats searchStats) {
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

    private void sendRefreshStats(String type, RefreshStats refreshStats) {
        sendInt(type, "total", refreshStats.getTotal());
        sendInt(type, "totalTimeInMillis", refreshStats.getTotalTimeInMillis());
        sendInt(type, "waitingListeners", refreshStats.getListeners());
    }

    private void sendIndexingStats(String type, IndexingStats indexingStats) {
        IndexingStats.Stats totalStats = indexingStats.getTotal();
        sendStats(type + "._all", totalStats);

        Map<String, IndexingStats.Stats> typeStats = indexingStats.getTypeStats();
        if (typeStats != null) {
            for (Map.Entry<String, IndexingStats.Stats> statsEntry : typeStats.entrySet()) {
                sendStats(type + "." + statsEntry.getKey(), statsEntry.getValue());
            }
        }
    }

    private void sendStats(String type, IndexingStats.Stats stats) {
        sendInt(type, "indexCount", stats.getIndexCount());
        sendInt(type, "indexTimeInMillis", stats.getIndexTime().millis());
        sendInt(type, "indexCurrent", stats.getIndexCount());
        sendInt(type, "deleteCount", stats.getDeleteCount());
        sendInt(type, "deleteTimeInMillis", stats.getDeleteTime().millis());
        sendInt(type, "deleteCurrent", stats.getDeleteCurrent());
        sendInt(type, "noopUpdateCount", stats.getNoopUpdateCount());
        sendInt(type, "throttleTimeInMillis", stats.getThrottleTime().getMillis());
        
    }

    private void sendGetStats(String type, GetStats getStats) {
        sendInt(type, "existsCount", getStats.getExistsCount());
        sendInt(type, "existsTimeInMillis", getStats.getExistsTimeInMillis());
        sendInt(type, "missingCount", getStats.getMissingCount());
        sendInt(type, "missingTimeInMillis", getStats.getMissingTimeInMillis());
        sendInt(type, "current", getStats.current());
        sendInt(type, "getCount", getStats.getCount());
        sendInt(type, "getTimeInMillis", getStats.getTimeInMillis());
    }

    private void sendFlushStats(String type, FlushStats flush) {
        sendInt(type, "total", flush.getTotal());
        sendInt(type, "totalTimeInMillis", flush.getTotalTimeInMillis());
    }

    private void sendDocsStats(String name, DocsStats docsStats) {
        sendInt(name, "count", docsStats.getCount());
        sendInt(name, "deleted", docsStats.getDeleted());
        sendInt(name, "averageSizeInBytes", docsStats.getAverageSizeInBytes());
        sendInt(name, "totalSizeInBytes", docsStats.getTotalSizeInBytes());
    }

    private void sendFieldDataStats(String name, FieldDataStats fieldDataStats) {
        sendInt(name, "memorySizeInBytes", fieldDataStats.getMemorySizeInBytes());
        sendInt(name, "evictions", fieldDataStats.getEvictions());
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
            writer.write(nameToSend);
            writer.write(' ');
            writer.write(value);
            writer.write(' ');
            writer.write(timestamp);
            writer.write('\n');
            writer.flush();
        } catch (IOException e) {
            logger.info("Error sending to Graphite:", e);
        }
    }

    protected void sendInt(String name, String valueName, long value) {
        sendToGraphite(name + "." + valueName, String.format("%d", value));
    }

    protected void sendInt(String name, String valueName, BigInteger value) {
        sendToGraphite(name + "." + valueName, value + "");
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

    private void flushWriter() {
        if (writer != null) {
            try {
                writer.flush();
            } catch (IOException e1) {
                logger.info("Error while flushing writer:", e1);
            }
        }
    }

    public Socket getSocket() throws Exception {
        SpecialPermission.check();
        return AccessController.doPrivileged(new PrivilegedAction<Socket>() {
            @Override
            public Socket run() {
                try {
                    return new Socket(host, port);                    
                }catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }            
        });
    }

    private void closeSocket(Socket socket) {
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException e) {
                logger.info("Error while closing socket:", e);
            }
        }
    }

    private void logException(Exception e) {
        logger.info("Error writing to Graphite", e);
    }
}
