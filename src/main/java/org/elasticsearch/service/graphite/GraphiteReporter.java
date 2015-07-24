package org.elasticsearch.service.graphite;

import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.http.HttpStats;
import org.elasticsearch.index.cache.filter.FilterCacheStats;
import org.elasticsearch.index.cache.id.IdCacheStats;
import org.elasticsearch.index.fielddata.FieldDataStats;
import org.elasticsearch.index.flush.FlushStats;
import org.elasticsearch.index.get.GetStats;
import org.elasticsearch.index.indexing.IndexingStats;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.refresh.RefreshStats;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.index.warmer.WarmerStats;
import org.elasticsearch.indices.NodeIndicesStats;
import org.elasticsearch.monitor.fs.FsStats;
import org.elasticsearch.monitor.jvm.JvmStats;
import org.elasticsearch.monitor.network.NetworkStats;
import org.elasticsearch.monitor.os.OsStats;
import org.elasticsearch.monitor.process.ProcessStats;
import org.elasticsearch.threadpool.ThreadPoolStats;
import org.elasticsearch.transport.TransportStats;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.Socket;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class GraphiteReporter {

    private static final ESLogger logger = ESLoggerFactory.getLogger(GraphiteReporter.class.getName());
    private Writer writer;
    private final String host;
    private final int port;
    private final String prefix;
    private List<IndexShard> indexShards;
    private NodeStats nodeStats;
    private final Pattern graphiteInclusionRegex;
    private final Pattern graphiteExclusionRegex;
    private final long timestamp;
    private final NodeIndicesStats nodeIndicesStats;


    public GraphiteReporter(String host, int port, String prefix, NodeIndicesStats nodeIndicesStats,
                            List<IndexShard> indexShards, NodeStats nodeStats,
                            Pattern graphiteInclusionRegex, Pattern graphiteExclusionRegex) {
        this.host = host;
        this.port = port;
        this.prefix = prefix;
        this.indexShards = indexShards;
        this.nodeStats = nodeStats;
        this.graphiteInclusionRegex = graphiteInclusionRegex;
        this.graphiteExclusionRegex = graphiteExclusionRegex;
        this.timestamp = System.currentTimeMillis() / 1000;
        this.nodeIndicesStats = nodeIndicesStats;
    }

    public void run() {
        Socket socket = null;
        try {
            socket = getSocket();
            writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));

            sendNodeIndicesStats();
            sendIndexShardStats();
            sendNodeStats();

            writer.flush();
        } catch (Exception e) {
            logException(e);
            flushWriter();
        } finally {
            closeSocket(socket);
            writer = null;
        }
    }

    private void sendNodeStats() {
        sendNodeFsStats(nodeStats.getFs());
        sendNodeHttpStats(nodeStats.getHttp());
        sendNodeJvmStats(nodeStats.getJvm());
        sendNodeNetworkStats(nodeStats.getNetwork());
        sendNodeOsStats(nodeStats.getOs());
        sendNodeProcessStats(nodeStats.getProcess());
        sendNodeTransportStats(nodeStats.getTransport());
        sendNodeThreadPoolStats(nodeStats.getThreadPool());
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
        sendInt(type, "rxSizeBytes", transportStats.rxSize().bytes());
        sendInt(type, "txCount", transportStats.txCount());
        sendInt(type, "txSizeBytes", transportStats.txSize().bytes());
    }

    private void sendNodeProcessStats(ProcessStats processStats) {
        String type = buildMetricName("node.process");

        sendInt(type, "openFileDescriptors", processStats.openFileDescriptors());
        if (processStats.cpu() != null) {
            sendInt(type + ".cpu", "percent", processStats.cpu().percent());
            sendInt(type + ".cpu", "sysSeconds", processStats.cpu().sys().seconds());
            sendInt(type + ".cpu", "totalSeconds", processStats.cpu().total().seconds());
            sendInt(type + ".cpu", "userSeconds", processStats.cpu().user().seconds());
        }

        if (processStats.mem() != null) {
            sendInt(type + ".mem", "totalVirtual", processStats.mem().totalVirtual().bytes());
            sendInt(type + ".mem", "resident", processStats.mem().resident().bytes());
            sendInt(type + ".mem", "share", processStats.mem().share().bytes());
        }
    }

    private void sendNodeOsStats(OsStats osStats) {
        String type = buildMetricName("node.os");

        if (osStats.cpu() != null) {
            sendInt(type + ".cpu", "sys", osStats.cpu().sys());
            sendInt(type + ".cpu", "idle", osStats.cpu().idle());
            sendInt(type + ".cpu", "user", osStats.cpu().user());
        }

        if (osStats.mem() != null) {
            sendInt(type + ".mem", "freeBytes", osStats.mem().free().bytes());
            sendInt(type + ".mem", "usedBytes", osStats.mem().used().bytes());
            sendInt(type + ".mem", "freePercent", osStats.mem().freePercent());
            sendInt(type + ".mem", "usedPercent", osStats.mem().usedPercent());
            sendInt(type + ".mem", "actualFreeBytes", osStats.mem().actualFree().bytes());
            sendInt(type + ".mem", "actualUsedBytes", osStats.mem().actualUsed().bytes());
        }

        if (osStats.swap() != null) {
            sendInt(type + ".swap", "freeBytes", osStats.swap().free().bytes());
            sendInt(type + ".swap", "usedBytes", osStats.swap().used().bytes());
        }
    }

    private void sendNodeNetworkStats(NetworkStats networkStats) {
        String type = buildMetricName("node.network.tcp");
        NetworkStats.Tcp tcp = networkStats.tcp();

        // might be null, if sigar isnt loaded
        if (tcp != null) {
            sendInt(type, "activeOpens", tcp.activeOpens());
            sendInt(type, "passiveOpens", tcp.passiveOpens());
            sendInt(type, "attemptFails", tcp.attemptFails());
            sendInt(type, "estabResets", tcp.estabResets());
            sendInt(type, "currEstab", tcp.currEstab());
            sendInt(type, "inSegs", tcp.inSegs());
            sendInt(type, "outSegs", tcp.outSegs());
            sendInt(type, "retransSegs", tcp.retransSegs());
            sendInt(type, "inErrs", tcp.inErrs());
            sendInt(type, "outRsts", tcp.outRsts());
        }
    }

    private void sendNodeJvmStats(JvmStats jvmStats) {
        String type = buildMetricName("node.jvm");
        sendInt(type, "uptime", jvmStats.uptime().seconds());

        // mem
        sendInt(type + ".mem", "heapCommitted", jvmStats.mem().heapCommitted().bytes());
        sendInt(type + ".mem", "heapUsed", jvmStats.mem().heapUsed().bytes());
        sendInt(type + ".mem", "nonHeapCommitted", jvmStats.mem().nonHeapCommitted().bytes());
        sendInt(type + ".mem", "nonHeapUsed", jvmStats.mem().nonHeapUsed().bytes());

        Iterator<JvmStats.MemoryPool> memoryPoolIterator = jvmStats.mem().iterator();
        while (memoryPoolIterator.hasNext()) {
            JvmStats.MemoryPool memoryPool = memoryPoolIterator.next();
            String memoryPoolType = type + ".mem.pool." + memoryPool.name();

            sendInt(memoryPoolType, "max", memoryPool.max().bytes());
            sendInt(memoryPoolType, "used", memoryPool.used().bytes());
            sendInt(memoryPoolType, "peakUsed", memoryPool.peakUsed().bytes());
            sendInt(memoryPoolType, "peakMax", memoryPool.peakMax().bytes());
        }

        // threads
        sendInt(type + ".threads", "count", jvmStats.threads().count());
        sendInt(type + ".threads", "peakCount", jvmStats.threads().peakCount());

        // garbage collectors
        for (JvmStats.GarbageCollector collector : jvmStats.gc().collectors()) {
            String id = type + ".gc." + collector.name();
            sendInt(id, "collectionCount", collector.collectionCount());
            sendInt(id, "collectionTimeSeconds", collector.collectionTime().seconds());

            JvmStats.GarbageCollector.LastGc lastGc = collector.lastGc();
            String lastGcType = type + ".lastGc";
            if (lastGc != null) {
                sendInt(lastGcType, "startTime", lastGc.startTime());
                sendInt(lastGcType, "endTime", lastGc.endTime());
                sendInt(lastGcType, "max", lastGc.max().bytes());
                sendInt(lastGcType, "beforeUsed", lastGc.beforeUsed().bytes());
                sendInt(lastGcType, "afterUsed", lastGc.afterUsed().bytes());
                sendInt(lastGcType, "durationSeconds", lastGc.duration().seconds());
            }
        }

        // TODO: bufferPools - where to get them?
    }

    private void sendNodeHttpStats(HttpStats httpStats) {
        String type = buildMetricName("node.http");
        sendInt(type, "serverOpen", httpStats.getServerOpen());
        sendInt(type, "totalOpen", httpStats.getTotalOpen());
    }

    private void sendNodeFsStats(FsStats fs) {
        Iterator<FsStats.Info> infoIterator = fs.iterator();
        int i = 0;
        while (infoIterator.hasNext()) {
            String type = buildMetricName("node.fs") + i;
            FsStats.Info info = infoIterator.next();
            sendInt(type, "available", info.getAvailable().bytes());
            sendInt(type, "total", info.getTotal().bytes());
            sendInt(type, "free", info.getFree().bytes());
            sendInt(type, "diskReads", info.getDiskReads());
            sendInt(type, "diskReadsInBytes", info.getDiskReadSizeInBytes());
            sendInt(type, "diskWrites", info.getDiskWrites());
            sendInt(type, "diskWritesInBytes", info.getDiskWriteSizeInBytes());
            sendFloat(type, "diskQueue", info.getDiskQueue());
            sendFloat(type, "diskService", info.getDiskServiceTime());
            i++;
        }
    }

    private void sendIndexShardStats() {
        for (IndexShard indexShard : indexShards) {
            String type = buildMetricName("indexes.") + indexShard.shardId().index().name() + ".id." + indexShard.shardId().id();
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
        sendInt(type, "throttleTimeInNanos", storeStats.throttleTime().getNanos());
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
        sendFilterCacheStats(type + ".filtercache", nodeIndicesStats.getFilterCache());
        sendIdCacheStats(type + ".idcache", nodeIndicesStats.getIdCache());
        sendDocsStats(type + ".docs", nodeIndicesStats.getDocs());
        sendFlushStats(type + ".flush", nodeIndicesStats.getFlush());
        sendGetStats(type + ".get", nodeIndicesStats.getGet());
        sendIndexingStats(type + ".indexing", nodeIndicesStats.getIndexing());
        sendRefreshStats(type + ".refresh", nodeIndicesStats.getRefresh());
        sendSearchStats(type + ".search", nodeIndicesStats.getSearch());
        sendFieldDataStats(type + ".fielddata", nodeIndicesStats.getFieldData());
    }

    private void sendSearchStats(String type, SearchStats searchStats) {
        SearchStats.Stats totalSearchStats = searchStats.getTotal();
        sendSearchStatsStats(type + "._all", totalSearchStats);

        if (searchStats.getGroupStats() != null ) {
            for (Map.Entry<String, SearchStats.Stats> statsEntry : searchStats.getGroupStats().entrySet()) {
                sendSearchStatsStats(type + "." + statsEntry.getKey(), statsEntry.getValue());
            }
        }
    }

    private void sendSearchStatsStats(String group, SearchStats.Stats searchStats) {
        String type = buildMetricName("search.stats.") + group;
        sendInt(type, "queryCount", searchStats.getQueryCount());
        sendInt(type, "queryTimeInMillis", searchStats.getQueryTimeInMillis());
        sendInt(type, "queryCurrent", searchStats.getQueryCurrent());
        sendInt(type, "fetchCount", searchStats.getFetchCount());
        sendInt(type, "fetchTimeInMillis", searchStats.getFetchTimeInMillis());
        sendInt(type, "fetchCurrent", searchStats.getFetchCurrent());
    }

    private void sendRefreshStats(String type, RefreshStats refreshStats) {
        sendInt(type, "total", refreshStats.getTotal());
        sendInt(type, "totalTimeInMillis", refreshStats.getTotalTimeInMillis());
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
        sendInt(type, "indexTimeInMillis", stats.getIndexTimeInMillis());
        sendInt(type, "indexCurrent", stats.getIndexCount());
        sendInt(type, "deleteCount", stats.getDeleteCount());
        sendInt(type, "deleteTimeInMillis", stats.getDeleteTimeInMillis());
        sendInt(type, "deleteCurrent", stats.getDeleteCurrent());
    }

    private void sendGetStats(String type, GetStats getStats) {
        sendInt(type, "existsCount", getStats.getExistsCount());
        sendInt(type, "existsTimeInMillis", getStats.getExistsTimeInMillis());
        sendInt(type, "missingCount", getStats.getMissingCount());
        sendInt(type, "missingTimeInMillis", getStats.getMissingTimeInMillis());
        sendInt(type, "current", getStats.current());
    }

    private void sendFlushStats(String type, FlushStats flush) {
        sendInt(type, "total", flush.getTotal());
        sendInt(type, "totalTimeInMillis", flush.getTotalTimeInMillis());
    }

    private void sendDocsStats(String name, DocsStats docsStats) {
        sendInt(name, "count", docsStats.getCount());
        sendInt(name, "deleted", docsStats.getDeleted());
    }

    private void sendIdCacheStats(String name, IdCacheStats idCache) {
        sendInt(name, "memorySizeInBytes", idCache.getMemorySizeInBytes());
    }

    private void sendFilterCacheStats(String name, FilterCacheStats filterCache) {
        sendInt(name, "memorySizeInBytes", filterCache.getMemorySizeInBytes());
        sendInt(name, "evictions", filterCache.getEvictions());
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
            writer.write(Long.toString(timestamp));
            writer.write('\n');
            writer.flush();
        } catch (IOException e) {
            logger.error("Error sending to Graphite:", e);
        }
    }

    protected void sendInt(String name, String valueName, long value) {
        sendToGraphite(name + "." + valueName, String.format("%d", value));
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
                logger.error("Error while flushing writer:", e1);
            }
        }
    }

    public Socket getSocket() throws Exception {
        return new Socket(host, port);
    }

    private void closeSocket(Socket socket) {
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException e) {
                logger.error("Error while closing socket:", e);
            }
        }
    }

    private void logException(Exception e) {
        if (logger.isDebugEnabled()) {
            logger.debug("Error writing to Graphite", e);
        } else {
            logger.warn("Error writing to Graphite: {}", e.getMessage());
        }
    }
}
