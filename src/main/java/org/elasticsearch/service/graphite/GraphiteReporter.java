package org.elasticsearch.service.graphite;

import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.http.HttpStats;
import org.elasticsearch.index.cache.CacheStats;
import org.elasticsearch.index.flush.FlushStats;
import org.elasticsearch.index.get.GetStats;
import org.elasticsearch.index.indexing.IndexingStats;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.refresh.RefreshStats;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.shard.service.IndexShard;
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

public class GraphiteReporter {

    private static final ESLogger logger = ESLoggerFactory.getLogger(GraphiteReporter.class.getName());
    private Writer writer;
    private final String host;
    private final int port;
    private String clusterName;
    private List<IndexShard> indexShards;
    private NodeStats nodeStats;
    private final long timestamp;
    private final NodeIndicesStats nodeIndicesStats;


    public GraphiteReporter(String host, int port, String clusterName, NodeIndicesStats nodeIndicesStats,
                            List<IndexShard> indexShards, NodeStats nodeStats) {
        this.host = host;
        this.port = port;
        this.clusterName = clusterName;
        this.indexShards = indexShards;
        this.nodeStats = nodeStats;
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
        sendNodeFsStats(nodeStats.fs());
        sendNodeHttpStats(nodeStats.http());
        sendNodeJvmStats(nodeStats.jvm());
        sendNodeNetworkStats(nodeStats.network());
        sendNodeOsStats(nodeStats.os());
        sendNodeProcessStats(nodeStats.process());
        sendNodeTransportStats(nodeStats.transport());
        sendNodeThreadPoolStats(nodeStats.threadPool());
    }

    private void sendNodeThreadPoolStats(ThreadPoolStats threadPoolStats) {
        String type = "elasticsearch." + clusterName + ".node.threadpool";
        Iterator<ThreadPoolStats.Stats> statsIterator = threadPoolStats.iterator();
        while (statsIterator.hasNext()) {
            ThreadPoolStats.Stats stats = statsIterator.next();
            String id = type + "." + stats.name();

            sendInt(id, "threads", stats.threads());
            sendInt(id, "queue", stats.queue());
            sendInt(id, "active", stats.active());
            sendInt(id, "rejected", stats.rejected());
            sendInt(id, "largest", stats.largest());
            sendInt(id, "completed", stats.completed());
        }
    }

    private void sendNodeTransportStats(TransportStats transportStats) {
        String type = "elasticsearch." + clusterName + ".node.transport";
        sendInt(type, "serverOpen", transportStats.serverOpen());
        sendInt(type, "rxCount", transportStats.rxCount());
        sendInt(type, "rxSizeBytes", transportStats.rxSize().bytes());
        sendInt(type, "txCount", transportStats.txCount());
        sendInt(type, "txSizeBytes", transportStats.txSize().bytes());
    }

    private void sendNodeProcessStats(ProcessStats processStats) {
        String type = "elasticsearch." + clusterName + ".node.process";

        sendInt(type, "openFileDescriptors", processStats.openFileDescriptors());
        sendInt(type + ".cpu", "percent", processStats.cpu().percent());
        sendInt(type + ".cpu", "sysSeconds", processStats.cpu().sys().seconds());
        sendInt(type + ".cpu", "totalSeconds", processStats.cpu().total().seconds());
        sendInt(type + ".cpu", "userSeconds", processStats.cpu().user().seconds());

        sendInt(type + ".mem", "totalVirtual", processStats.mem().totalVirtual().bytes());
        sendInt(type + ".mem", "resident", processStats.mem().resident().bytes());
        sendInt(type + ".mem", "share", processStats.mem().share().bytes());
    }

    private void sendNodeOsStats(OsStats osStats) {
        String type = "elasticsearch." + clusterName + ".node.os";

        sendInt(type + ".cpu", "sys", osStats.cpu().sys());
        sendInt(type + ".cpu", "idle", osStats.cpu().idle());
        sendInt(type + ".cpu", "user", osStats.cpu().user());

        sendInt(type + ".mem", "freeBytes", osStats.mem().free().bytes());
        sendInt(type + ".mem", "usedBytes", osStats.mem().used().bytes());
        sendInt(type + ".mem", "freePercent", osStats.mem().freePercent());
        sendInt(type + ".mem", "usedPercent", osStats.mem().usedPercent());
        sendInt(type + ".mem", "actualFreeBytes", osStats.mem().actualFree().bytes());
        sendInt(type + ".mem", "actualUsedBytes", osStats.mem().actualUsed().bytes());

        sendInt(type + ".swap", "freeBytes", osStats.swap().free().bytes());
        sendInt(type + ".swap", "usedBytes", osStats.swap().used().bytes());
    }

    private void sendNodeNetworkStats(NetworkStats networkStats) {
        String type = "elasticsearch." + clusterName + ".node.network.tcp";
        NetworkStats.Tcp tcp = networkStats.tcp();

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

    private void sendNodeJvmStats(JvmStats jvmStats) {
        String type = "elasticsearch." + clusterName + ".node.jvm";
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
        sendInt(type + ".gc", "collectionCount", jvmStats.gc().collectionCount());
        sendInt(type + ".gc", "collectionTimeSeconds", jvmStats.gc().collectionTime().seconds());
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
        String type = "elasticsearch." + clusterName + ".node.http";
        sendInt(type, "serverOpen", httpStats.serverOpen());
        sendInt(type, "totalOpen", httpStats.totalOpen());
    }

    private void sendNodeFsStats(FsStats fs) {
        Iterator<FsStats.Info> infoIterator = fs.iterator();
        int i = 0;
        while (infoIterator.hasNext()) {
            String type = "elasticsearch." + clusterName + ".node.fs." + i;
            FsStats.Info info = infoIterator.next();
            sendInt(type, "available", info.available().bytes());
            sendInt(type, "total", info.total().bytes());
            sendInt(type, "free", info.free().bytes());
            sendInt(type, "diskReads", info.diskReads());
            sendInt(type, "diskReadsInBytes", info.diskReadSizeInBytes());
            sendInt(type, "diskWrites", info.diskWrites());
            sendInt(type, "diskWritesInBytes", info.diskWriteSizeInBytes());
            sendFloat(type, "diskQueue", info.diskQueue());
            sendFloat(type, "diskService", info.diskServiceTime());
            i++;
        }
    }

    private void sendIndexShardStats() {
        for (IndexShard indexShard : indexShards) {
            String type = "elasticsearch." + clusterName + ".indexes." + indexShard.shardId().index().name() + ".id." + indexShard.shardId().id();
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
        sendInt(type, "total", mergeStats.total());
        sendInt(type, "totalTimeInMillis", mergeStats.totalTimeInMillis());
        sendInt(type, "totalNumDocs", mergeStats.totalNumDocs());
        sendInt(type, "current", mergeStats.current());
        sendInt(type, "currentNumDocs", mergeStats.currentNumDocs());
        sendInt(type, "currentSizeInBytes", mergeStats.currentSizeInBytes());
    }

    private void sendNodeIndicesStats() {
        String type = "elasticsearch." + clusterName + ".node";
        sendCacheStats(type + ".cache", nodeIndicesStats.cache());
        sendDocsStats(type + ".docs", nodeIndicesStats.docs());
        sendFlushStats(type + ".flush", nodeIndicesStats.flush());
        sendGetStats(type + ".get", nodeIndicesStats.get());
        sendIndexingStats(type + ".indexing", nodeIndicesStats.indexing());
        sendRefreshStats(type + ".refresh", nodeIndicesStats.refresh());
        sendSearchStats(type + ".search", nodeIndicesStats.search());
    }

    private void sendSearchStats(String type, SearchStats searchStats) {
        SearchStats.Stats totalSearchStats = searchStats.total();
        sendSearchStatsStats(type + "._all", totalSearchStats);

        if (searchStats.groupStats() != null ) {
            for (Map.Entry<String, SearchStats.Stats> statsEntry : searchStats.groupStats().entrySet()) {
                sendSearchStatsStats(type + "." + statsEntry.getKey(), statsEntry.getValue());
            }
        }
    }

    private void sendSearchStatsStats(String group, SearchStats.Stats searchStats) {
        String type = "elasticsearch." + clusterName + ".search.stats." + group;
        sendInt(type, "queryCount", searchStats.queryCount());
        sendInt(type, "queryTimeInMillis", searchStats.queryTimeInMillis());
        sendInt(type, "queryCurrent", searchStats.queryCurrent());
        sendInt(type, "fetchCount", searchStats.fetchCount());
        sendInt(type, "fetchTimeInMillis", searchStats.fetchTimeInMillis());
        sendInt(type, "fetchCurrent", searchStats.fetchCurrent());
    }

    private void sendRefreshStats(String type, RefreshStats refreshStats) {
        sendInt(type, "total", refreshStats.total());
        sendInt(type, "totalTimeInMillis", refreshStats.totalTimeInMillis());
    }

    private void sendIndexingStats(String type, IndexingStats indexingStats) {
        IndexingStats.Stats totalStats = indexingStats.total();
        sendStats(type + "._all", totalStats);

        Map<String, IndexingStats.Stats> typeStats = indexingStats.typeStats();
        if (typeStats != null) {
            for (Map.Entry<String, IndexingStats.Stats> statsEntry : typeStats.entrySet()) {
                sendStats(type + "." + statsEntry.getKey(), statsEntry.getValue());
            }
        }
    }

    private void sendStats(String type, IndexingStats.Stats stats) {
        sendInt(type, "indexCount", stats.indexCount());
        sendInt(type, "indexTimeInMillis", stats.indexTimeInMillis());
        sendInt(type, "indexCurrent", stats.indexCurrent());
        sendInt(type, "deleteCount", stats.deleteCount());
        sendInt(type, "deleteTimeInMillis", stats.deleteTimeInMillis());
        sendInt(type, "deleteCurrent", stats.deleteCurrent());
    }

    private void sendGetStats(String type, GetStats getStats) {
        sendInt(type, "existsCount", getStats.existsCount());
        sendInt(type, "existsTimeInMillis", getStats.existsTimeInMillis());
        sendInt(type, "missingCount", getStats.missingCount());
        sendInt(type, "missingTimeInMillis", getStats.missingTimeInMillis());
        sendInt(type, "current", getStats.current());
    }

    private void sendFlushStats(String type, FlushStats flush) {
        sendInt(type, "total", flush.total());
        sendInt(type, "totalTimeInMillis", flush.totalTimeInMillis());
    }

    private void sendDocsStats(String name, DocsStats docsStats) {
        sendInt(name, "count", docsStats.count());
        sendInt(name, "deleted", docsStats.deleted());
    }

    private void sendCacheStats(String name, CacheStats cacheStats) {
        sendInt(name, "filterEvictions", cacheStats.filterEvictions());
        sendInt(name, "filterMemEvictions", cacheStats.filterMemEvictions());
        sendInt(name, "filterCount", cacheStats.filterCount());
        sendInt(name, "fieldSizeInBytes", cacheStats.fieldSizeInBytes());
        sendInt(name, "filterSizeInBytes", cacheStats.filterSizeInBytes());
        sendInt(name, "bloomSizeInBytes", cacheStats.bloomSizeInBytes());
        sendInt(name, "idCacheSizeInBytes", cacheStats.idCacheSizeInBytes());
    }


    protected void sendToGraphite(String name, String value) {
        try {
            writer.write(sanitizeString(name));
            writer.write('.');
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
        sendToGraphite(name, valueName + " " + String.format("%d", value));
    }

    protected void sendFloat(String name, String valueName, double value) {
        sendToGraphite(name, valueName + " " + String.format("%2.2f", value));
    }

//    protected void sendObjToGraphite(String name, String valueName, Object value) {
//        sendToGraphite(name, valueName + " " + String.format("%s", value));
//    }

    protected String sanitizeString(String s) {
        return s.replace(' ', '-');
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
