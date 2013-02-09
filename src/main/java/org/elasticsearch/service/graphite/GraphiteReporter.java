package org.elasticsearch.service.graphite;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.Socket;
import java.util.List;
import java.util.Map;

public class GraphiteReporter {

    private static final ESLogger logger = ESLoggerFactory.getLogger(GraphiteReporter.class.getName());
    private Writer writer;
    private final String host;
    private final int port;
    private String clusterName;
    private List<IndexShard> indexShards;
    private final long timestamp;
    private final NodeIndicesStats nodeIndicesStats;


    public GraphiteReporter(String host, int port, String clusterName, NodeIndicesStats nodeIndicesStats, List<IndexShard> indexShards) {
        this.host = host;
        this.port = port;
        this.clusterName = clusterName;
        this.indexShards = indexShards;
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

            writer.flush();
        } catch (Exception e) {
            logException(e);
            flushWriter();
        } finally {
            closeSocket(socket);
            writer = null;
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

//    protected void sendFloat(String name, String valueName, double value) {
//        sendToGraphite(name, valueName + " " + String.format("%2.2f", value));
//    }
//
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
