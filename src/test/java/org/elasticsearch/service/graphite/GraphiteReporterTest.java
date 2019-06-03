package org.elasticsearch.service.graphite;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.StringWriter;
import java.util.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.common.FieldMemoryStats;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.discovery.DiscoveryStats;
import org.elasticsearch.discovery.zen.PendingClusterStateStats;
import org.elasticsearch.discovery.zen.PublishClusterStateStats;
import org.elasticsearch.http.HttpStats;
import org.elasticsearch.index.cache.bitset.ShardBitsetFilterCache;
import org.elasticsearch.index.cache.query.QueryCacheStats;
import org.elasticsearch.index.cache.request.RequestCacheStats;
import org.elasticsearch.index.cache.request.ShardRequestCache;
import org.elasticsearch.index.engine.SegmentsStats;
import org.elasticsearch.index.fielddata.FieldDataStats;
import org.elasticsearch.index.flush.FlushStats;
import org.elasticsearch.index.get.GetStats;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.recovery.RecoveryStats;
import org.elasticsearch.index.refresh.RefreshStats;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.search.stats.SearchStats.Stats;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.IndexingStats;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.index.translog.TranslogStats;
import org.elasticsearch.index.warmer.WarmerStats;
import org.elasticsearch.indices.NodeIndicesStats;
import org.elasticsearch.indices.breaker.AllCircuitBreakerStats;
import org.elasticsearch.indices.breaker.CircuitBreakerStats;
import org.elasticsearch.ingest.IngestStats;
import org.elasticsearch.monitor.fs.FsInfo;
import org.elasticsearch.monitor.fs.FsInfo.DeviceStats;
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
import org.junit.Before;
import org.junit.Test;

import com.carrotsearch.hppc.ObjectLongHashMap;

public class GraphiteReporterTest{
    
    private static final Logger log = LogManager.getLogger(GraphiteReporterTest.class); 
    private NodeIndicesStats nodeIndicesStats;
    private List<IndexShard> shards;
    private NodeStats nodeStats;
    private String prefix = "someprefix";
    private StringWriter stringWriter = new StringWriter();
    private StatsWriter statsWriter;
    private GraphiteReporter reporter;
    
    @Before
    public void beforeLocal() {
        statsWriter = new StatsWriter() {
            public void write(String message) throws IOException {
                stringWriter.write(message);
            }
            public void write(char c) throws IOException {
                stringWriter.write(c);
            }
            public void open() throws IOException {
            }
            public void flushAndClose() {
            }
            public void flush() throws IOException {
            }
        };
        
        nodeIndicesStats = mock(NodeIndicesStats.class);
        nodeStats = mock(NodeStats.class);
        IndexShard indexShard = mock(IndexShard.class);
        ShardId shardId = mock(ShardId.class);
        when(shardId.getIndex()).thenReturn(new org.elasticsearch.index.Index("indexname", UUID.randomUUID().toString()));
        when(indexShard.shardId()).thenReturn(shardId);
        
        shards = Collections.singletonList(indexShard);

        reporter = new GraphiteReporter(statsWriter, prefix, nodeIndicesStats, shards, nodeStats, null, null, "1", System.currentTimeMillis(), new StopWatch()); 
    }
    
    @Test
    public void testSendScriptStats() {        
        ScriptStats stats = new ScriptStats(1, 2);
        
        reporter.sendScriptStats(stats);
        
        assertThat(stringWriter.toString(), containsString(prefix + ".node.script.cacheEvictions"));
        assertThat(stringWriter.toString(), containsString(prefix + ".node.script.compilations"));
        
        assertEquals(stringWriter.toString().split("\n").length, 2);
    }

    @Test
    public void testSendIngestStatsIngestStats() {

        final IngestStats.Stats totalStats = new IngestStats.Stats(1, 2,3 ,4);
        final IngestStats.PipelineStat pipelineStat = new IngestStats.PipelineStat("pipeline1", totalStats);
        final IngestStats stats = new IngestStats(totalStats, Collections.singletonList(pipelineStat),
                Collections.emptyMap());

        reporter.sendIngestStats(stats);
        
        assertThat(stringWriter.toString(), containsString(prefix + ".node.ingest.total.ingestCount"));
        assertThat(stringWriter.toString(), containsString(prefix + ".node.ingest.total.ingestFailedCount"));
        assertThat(stringWriter.toString(), containsString(prefix + ".node.ingest.total.ingestTimeInMillis"));
        assertThat(stringWriter.toString(), containsString(prefix + ".node.ingest.total.ingestCurrent"));
        
        assertThat(stringWriter.toString(), containsString(prefix + ".node.ingest.pipeline1.ingestCount"));
        assertThat(stringWriter.toString(), containsString(prefix + ".node.ingest.pipeline1.ingestFailedCount"));
        assertThat(stringWriter.toString(), containsString(prefix + ".node.ingest.pipeline1.ingestTimeInMillis"));
        assertThat(stringWriter.toString(), containsString(prefix + ".node.ingest.pipeline1.ingestCurrent"));
        
        assertEquals(stringWriter.toString().split("\n").length, 8);
    }
    
    @Test
    public void testSendAdaptiveSelectionStats() {
        Map<String, Long> a1 = Collections.singletonMap("1",1L);
        Map<String, ComputedNodeStats> a2 = Collections.singletonMap("1", new ComputedNodeStats("1", 1,2,3,4));
        AdaptiveSelectionStats stats = new AdaptiveSelectionStats(a1, a2);
        
        reporter.sendAdaptiveSelectionStats(stats);
        
        assertThat(stringWriter.toString(), containsString(prefix + ".node.selection.computed.queueSize"));
        assertThat(stringWriter.toString(), containsString(prefix + ".node.selection.computed.responseTime"));
        assertThat(stringWriter.toString(), containsString(prefix + ".node.selection.computed.serviceTime"));
        
        assertEquals(stringWriter.toString().split("\n").length, 3);
    }

    @Test
    public void testSendDiscoveryStats() {
        PendingClusterStateStats a1 = new PendingClusterStateStats(1,2,3);
        PublishClusterStateStats a2 = new PublishClusterStateStats(4, 5, 6);
        DiscoveryStats stats = new DiscoveryStats(a1, a2);
        
        reporter.sendDiscoveryStats(stats);
        
        assertThat(stringWriter.toString(), containsString(prefix + ".node.discovery.queue.committed"));
        assertThat(stringWriter.toString(), containsString(prefix + ".node.discovery.queue.pending"));
        assertThat(stringWriter.toString(), containsString(prefix + ".node.discovery.queue.total"));
        
        assertEquals(stringWriter.toString().split("\n").length, 3);
    }

    @Test
    public void testSendNodeBreakerStats() {
        
        CircuitBreakerStats a1 = new CircuitBreakerStats("name", 1, 2, 3, 4);
        AllCircuitBreakerStats stats = new AllCircuitBreakerStats(new CircuitBreakerStats[] {a1});
        
        reporter.sendNodeBreakerStats(stats);
        
        assertThat(stringWriter.toString(), containsString(prefix + ".node.breaker.name.estimated"));
        assertThat(stringWriter.toString(), containsString(prefix + ".node.breaker.name.limit"));
        assertThat(stringWriter.toString(), containsString(prefix + ".node.breaker.name.trippedCount"));
        assertThat(stringWriter.toString(), containsString(prefix + ".node.breaker.name.overhead"));
        
        assertEquals(stringWriter.toString().split("\n").length, 4);
    }

    @Test
    public void testSendNodeThreadPoolStats() {
        ThreadPoolStats stats = new ThreadPoolStats(Collections.singletonList(new ThreadPoolStats.Stats("tp-name", 1, 2, 3, 4, 5, 6)));
                
        reporter.sendNodeThreadPoolStats(stats);
        
        assertThat(stringWriter.toString(), containsString(prefix + ".node.threadpool.tp-name.threads"));
        assertThat(stringWriter.toString(), containsString(prefix + ".node.threadpool.tp-name.queue"));
        assertThat(stringWriter.toString(), containsString(prefix + ".node.threadpool.tp-name.active"));
        assertThat(stringWriter.toString(), containsString(prefix + ".node.threadpool.tp-name.rejected"));
        assertThat(stringWriter.toString(), containsString(prefix + ".node.threadpool.tp-name.largest"));
        assertThat(stringWriter.toString(), containsString(prefix + ".node.threadpool.tp-name.completed"));
        
        assertEquals(stringWriter.toString().split("\n").length, 6);
    }
    

    @Test
    public void testSendNodeTransportStats() {
        TransportStats stats = new TransportStats(1, 2, 3, 4, 5);
        
        reporter.sendNodeTransportStats(stats);
        
        assertThat(stringWriter.toString(), containsString(prefix + ".node.transport.serverOpen"));
        assertThat(stringWriter.toString(), containsString(prefix + ".node.transport.rxCount"));
        assertThat(stringWriter.toString(), containsString(prefix + ".node.transport.rxSizeBytes"));
        assertThat(stringWriter.toString(), containsString(prefix + ".node.transport.txCount"));
        assertThat(stringWriter.toString(), containsString(prefix + ".node.transport.txSizeBytes"));
        
        assertEquals(stringWriter.toString().split("\n").length, 5);
    }

    @Test
    public void testSendNodeProcessStats() {
        ProcessStats stats = new ProcessStats(1, 2, 3, new ProcessStats.Cpu((short)4, 5), new ProcessStats.Mem(1));
        
        reporter.sendNodeProcessStats(stats);
        
        assertThat(stringWriter.toString(), containsString(prefix + ".node.process.openFileDescriptors"));
        assertThat(stringWriter.toString(), containsString(prefix + ".node.process.maxFileDescriptors"));
        assertThat(stringWriter.toString(), containsString(prefix + ".node.process.cpu.percent"));
        assertThat(stringWriter.toString(), containsString(prefix + ".node.process.cpu.total_seconds"));
        assertThat(stringWriter.toString(), containsString(prefix + ".node.process.mem.totalVirtual"));
        
        assertEquals(stringWriter.toString().split("\n").length, 5);        
    }

    @Test
    public void testSendNodeOsStats() {
        OsStats.Cpu a2 = new OsStats.Cpu((short)2, new double[] {3,4,5});
        OsStats.Mem a3 = new OsStats.Mem(6, 7);
        OsStats.Swap a4 = new OsStats.Swap(8, 9);
        OsStats.Cgroup.CpuStat a5 = new OsStats.Cgroup.CpuStat(13,  14,  16);
        OsStats.Cgroup a6 = new OsStats.Cgroup("g1", 10, "g2", 11, 12, a5, "g3", "1000000000000", "2000000000000" );
        OsStats stats = new OsStats(17, a2, a3, a4, a6);
                
        reporter.sendNodeOsStats(stats);
        
        assertThat(stringWriter.toString(), containsString(prefix + ".node.os.cpu.percent"));
        assertThat(stringWriter.toString(), containsString(prefix + ".node.os.cpu.load_1_min"));
        assertThat(stringWriter.toString(), containsString(prefix + ".node.os.cpu.load_5_min"));
        assertThat(stringWriter.toString(), containsString(prefix + ".node.os.cpu.load_15_min"));
        
        assertThat(stringWriter.toString(), containsString(prefix + ".node.os.cgroup.cpuAcctUsageNanos"));
        assertThat(stringWriter.toString(), containsString(prefix + ".node.os.cgroup.cpuCfsPeriodMicros"));
        assertThat(stringWriter.toString(), containsString(prefix + ".node.os.cgroup.cpuCfsQuotaMicros"));
        assertThat(stringWriter.toString(), containsString(prefix + ".node.os.cgroup.memoryLimitInBytes"));
        assertThat(stringWriter.toString(), containsString(prefix + ".node.os.cgroup.memoryUsageInBytes"));
        
        assertThat(stringWriter.toString(), containsString(prefix + ".node.os.cgroup.cpu.numberOfElapsedPeriods"));
        assertThat(stringWriter.toString(), containsString(prefix + ".node.os.cgroup.cpu.numberOfTimesThrottled"));
        assertThat(stringWriter.toString(), containsString(prefix + ".node.os.cgroup.cpu.timeThrottledNanos"));
        
        assertThat(stringWriter.toString(), containsString(prefix + ".node.os.mem.freeBytes"));
        assertThat(stringWriter.toString(), containsString(prefix + ".node.os.mem.usedBytes"));
        assertThat(stringWriter.toString(), containsString(prefix + ".node.os.mem.totalBytes"));
        assertThat(stringWriter.toString(), containsString(prefix + ".node.os.mem.freePercent"));
        assertThat(stringWriter.toString(), containsString(prefix + ".node.os.mem.usedPercent"));
        
        assertThat(stringWriter.toString(), containsString(prefix + ".node.os.swap.freeBytes"));
        assertThat(stringWriter.toString(), containsString(prefix + ".node.os.swap.usedBytes"));
        assertThat(stringWriter.toString(), containsString(prefix + ".node.os.swap.totalBytes"));
        
        assertEquals(stringWriter.toString().split("\n").length, 20);
    }

    @Test
    public void testSendNodeJvmStats() {
        reporter.sendNodeJvmStats(JvmStats.jvmStats());
        
        assertThat(stringWriter.toString(), containsString(prefix + ".node.jvm.uptime"));
        
        assertThat(stringWriter.toString(), containsString(prefix + ".node.jvm.mem.heapCommitted"));
        assertThat(stringWriter.toString(), containsString(prefix + ".node.jvm.mem.heapUsed"));
        assertThat(stringWriter.toString(), containsString(prefix + ".node.jvm.mem.nonHeapCommitted"));
        assertThat(stringWriter.toString(), containsString(prefix + ".node.jvm.mem.nonHeapUsed"));
        
        assertThat(stringWriter.toString(), containsString(prefix + ".node.jvm.mem.pool.young.max"));
        assertThat(stringWriter.toString(), containsString(prefix + ".node.jvm.mem.pool.young.used"));
        assertThat(stringWriter.toString(), containsString(prefix + ".node.jvm.mem.pool.young.peakUsed"));
        assertThat(stringWriter.toString(), containsString(prefix + ".node.jvm.mem.pool.young.peakMax"));
        
        assertThat(stringWriter.toString(), containsString(prefix + ".node.jvm.mem.pool.survivor.max"));
        assertThat(stringWriter.toString(), containsString(prefix + ".node.jvm.mem.pool.survivor.used"));
        assertThat(stringWriter.toString(), containsString(prefix + ".node.jvm.mem.pool.survivor.peakUsed"));
        assertThat(stringWriter.toString(), containsString(prefix + ".node.jvm.mem.pool.survivor.peakMax"));
        
        assertThat(stringWriter.toString(), containsString(prefix + ".node.jvm.mem.pool.old.max"));
        assertThat(stringWriter.toString(), containsString(prefix + ".node.jvm.mem.pool.old.used"));
        assertThat(stringWriter.toString(), containsString(prefix + ".node.jvm.mem.pool.old.peakUsed"));
        assertThat(stringWriter.toString(), containsString(prefix + ".node.jvm.mem.pool.old.peakMax"));
        
        assertThat(stringWriter.toString(), containsString(prefix + ".node.jvm.threads.count"));
        assertThat(stringWriter.toString(), containsString(prefix + ".node.jvm.threads.peakCount"));
        
        assertThat(stringWriter.toString(), containsString(prefix + ".node.jvm.gc.young.collectionCount"));
        assertThat(stringWriter.toString(), containsString(prefix + ".node.jvm.gc.young.collectionTimeSeconds"));
        
        assertThat(stringWriter.toString(), containsString(prefix + ".node.jvm.gc.old.collectionCount"));
        assertThat(stringWriter.toString(), containsString(prefix + ".node.jvm.gc.old.collectionTimeSeconds"));
        
        assertThat(stringWriter.toString(), containsString(prefix + ".node.jvm.classes.loadedClassCount"));
        assertThat(stringWriter.toString(), containsString(prefix + ".node.jvm.classes.totalLoadedClassCount"));
        assertThat(stringWriter.toString(), containsString(prefix + ".node.jvm.classes.unloadedClassCount"));
        
        assertEquals(stringWriter.toString().split("\n").length, 26);
        
    }
    
    @Test
    public void testSendNodeHttpStats() {
        HttpStats stats = new HttpStats(1, 2);
        
        reporter.sendNodeHttpStats(stats);
        
        assertThat(stringWriter.toString(), containsString(prefix + ".node.http.serverOpen"));
        assertThat(stringWriter.toString(), containsString(prefix + ".node.http.totalOpen"));
        
        assertEquals(stringWriter.toString().split("\n").length, 2);
    }

    @Test
    public void testSendNodeFsStats() {
        DeviceStats a1[] = new DeviceStats[] {new DeviceStats(1, 1, "dname", 1, 1, 1, 1, null)};
        Path a2[] = new Path[]{new Path("/", "/mnt", 1, 2, 3)};
        FsInfo stats = new FsInfo(1, new IoStats(a1), a2);
        
        reporter.sendNodeFsStats(stats);
        log.debug("metrics: \n" + stringWriter);
        
        assertThat(stringWriter.toString(), containsString(prefix + ".node.fs0.available"));
        assertThat(stringWriter.toString(), containsString(prefix + ".node.fs0.total"));
        assertThat(stringWriter.toString(), containsString(prefix + ".node.fs0.free"));
        
        assertThat(stringWriter.toString(), containsString(prefix + ".node.io.iops.total"));
        assertThat(stringWriter.toString(), containsString(prefix + ".node.io.iops.read"));
        assertThat(stringWriter.toString(), containsString(prefix + ".node.io.iops.write"));
        assertThat(stringWriter.toString(), containsString(prefix + ".node.io.readInKiloBytes"));
        assertThat(stringWriter.toString(), containsString(prefix + ".node.io.writeInKiloBytes"));
        assertEquals(stringWriter.toString().split("\n").length, 8);
    }

    @Test
    public void testSendIndexShardStatsStringIndexShard() {

        IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.state()).thenReturn(IndexShardState.STARTED);
        when(indexShard.shardBitsetFilterCache()).thenReturn(mock(ShardBitsetFilterCache.class));
        when(indexShard.requestCache()).thenReturn(new ShardRequestCache());
        
        reporter.sendIndexShardStats("sometype", indexShard);
        log.debug("metrics: \n" + stringWriter);
        
        verify(indexShard).searchStats("_all");
        verify(indexShard).getStats();
        verify(indexShard).docStats();
        verify(indexShard).refreshStats();
        verify(indexShard).indexingStats("_all");
        verify(indexShard).mergeStats();
        verify(indexShard).warmerStats();
        verify(indexShard).storeStats();
        verify(indexShard).flushStats();
        verify(indexShard).recoveryStats();
        verify(indexShard).requestCache();
        verify(indexShard).translogStats();
        verify(indexShard).fieldDataStats("*");
        
        assertThat(stringWriter.toString(), containsString("sometype.indexBufferRAMBytesUsed"));
        assertThat(stringWriter.toString(), containsString("sometype.activeOperationsCount"));
        assertThat(stringWriter.toString(), containsString("sometype.writingBytes"));
        assertThat(stringWriter.toString(), containsString("sometype.shardBitsetFilterCacheSizeInBytes"));
        
    }
    
    @Test
    public void testSendStoreStats() {
        StoreStats stats = new StoreStats(1);
        
        reporter.sendStoreStats("sometype", stats);

        assertThat(stringWriter.toString(), containsString("sometype.sizeInBytes"));
        
        assertEquals(stringWriter.toString().split("\n").length, 1);
    }

    @Test
    public void testSendWarmerStats() {
        WarmerStats stats = new WarmerStats(1, 2, 3);
        
        reporter.sendWarmerStats("sometype", stats);

        assertThat(stringWriter.toString(), containsString("sometype.current"));
        assertThat(stringWriter.toString(), containsString("sometype.total"));
        assertThat(stringWriter.toString(), containsString("sometype.totalTimeInMillis"));

        assertEquals(stringWriter.toString().split("\n").length, 3);
    }

    @Test
    public void testSendMergeStats() {
        MergeStats stats = new MergeStats();
        
        reporter.sendMergeStats("sometype", stats);
        
        assertThat(stringWriter.toString(), containsString("sometype.total"));
        assertThat(stringWriter.toString(), containsString("sometype.totalTimeInMillis"));
        assertThat(stringWriter.toString(), containsString("sometype.totalNumDocs"));
        assertThat(stringWriter.toString(), containsString("sometype.current"));
        assertThat(stringWriter.toString(), containsString("sometype.currentNumDocs"));
        assertThat(stringWriter.toString(), containsString("sometype.currentSizeInBytes"));
        
        assertEquals(stringWriter.toString().split("\n").length, 6);
    }

    @Test
    public void testSendQueryCacheStats() {
        reporter.sendQueryCacheStats("sometype", new QueryCacheStats());
        
        assertThat(stringWriter.toString(), containsString("sometype.evictions"));
        assertThat(stringWriter.toString(), containsString("sometype.hitCount"));
        assertThat(stringWriter.toString(), containsString("sometype.memorySizeInBytes"));
        assertThat(stringWriter.toString(), containsString("sometype.missCount"));
        
        assertEquals(stringWriter.toString().split("\n").length, 4);
    }

    @Test
    public void testSendSegmentsStats() {
        reporter.sendSegmentsStats("sometype", new SegmentsStats());
        
        assertThat(stringWriter.toString(), containsString("sometype.count"));
        assertThat(stringWriter.toString(), containsString("sometype.docValuesMemory"));
        assertThat(stringWriter.toString(), containsString("sometype.bitSetMemoryInBytes"));
        assertThat(stringWriter.toString(), containsString("sometype.indexWriterMemoryInBytes"));
        assertThat(stringWriter.toString(), containsString("sometype.memoryInBytes"));
        
        assertThat(stringWriter.toString(), containsString("sometype.versionMapMemoryInBytes"));
        assertThat(stringWriter.toString(), containsString("sometype.normsMemoryInBytes"));
        assertThat(stringWriter.toString(), containsString("sometype.pointsMemoryInBytes"));
        assertThat(stringWriter.toString(), containsString("sometype.storedFieldsMemoryInBytes"));        
        assertThat(stringWriter.toString(), containsString("sometype.termsMemoryInBytes"));
        
        assertThat(stringWriter.toString(), containsString("sometype.termVectorsMemoryInBytes"));
        
        assertEquals(stringWriter.toString().split("\n").length, 11);
    }

    @Test
    public void testSendSearchStats() {
        reporter.sendSearchStats("sometype", new SearchStats());
        
        assertThat(stringWriter.toString(), containsString("sometype.openContexts"));
        
        assertThat(stringWriter.toString(), containsString("sometype._all.queryCount"));
        assertThat(stringWriter.toString(), containsString("sometype._all.queryTimeInMillis"));
        assertThat(stringWriter.toString(), containsString("sometype._all.queryCurrent"));
        
        assertThat(stringWriter.toString(), containsString("sometype._all.fetchCount"));
        assertThat(stringWriter.toString(), containsString("sometype._all.fetchTimeInMillis"));
        assertThat(stringWriter.toString(), containsString("sometype._all.fetchCurrent"));
        
        assertThat(stringWriter.toString(), containsString("sometype._all.scrollCount"));
        assertThat(stringWriter.toString(), containsString("sometype._all.scrollTimeInMillis"));
        assertThat(stringWriter.toString(), containsString("sometype._all.scrollCurrent"));
        
        assertThat(stringWriter.toString(), containsString("sometype._all.suggestCount"));
        assertThat(stringWriter.toString(), containsString("sometype._all.suggestTimeInMillis"));
        assertThat(stringWriter.toString(), containsString("sometype._all.suggestCurrent"));
        
        assertEquals(stringWriter.toString().split("\n").length, 13);
        
    }

    @Test
    public void testSendSearchStatsStats() {
        Stats stats = new SearchStats().getTotal();
        reporter.sendSearchStatsStats("sometype", stats);
        
        assertThat(stringWriter.toString(), containsString("sometype.queryCount"));
        assertThat(stringWriter.toString(), containsString("sometype.queryTimeInMillis"));
        assertThat(stringWriter.toString(), containsString("sometype.queryCurrent"));
        
        assertThat(stringWriter.toString(), containsString("sometype.fetchCount"));
        assertThat(stringWriter.toString(), containsString("sometype.fetchTimeInMillis"));
        assertThat(stringWriter.toString(), containsString("sometype.fetchCurrent"));
        
        assertThat(stringWriter.toString(), containsString("sometype.scrollCount"));
        assertThat(stringWriter.toString(), containsString("sometype.scrollTimeInMillis"));
        assertThat(stringWriter.toString(), containsString("sometype.scrollCurrent"));
        
        assertThat(stringWriter.toString(), containsString("sometype.suggestCount"));
        assertThat(stringWriter.toString(), containsString("sometype.suggestTimeInMillis"));
        assertThat(stringWriter.toString(), containsString("sometype.suggestCurrent"));

        assertEquals(stringWriter.toString().split("\n").length, 12);
    }

    @Test
    public void testSendRefreshStats() {
        reporter.sendRefreshStats("sometype", new RefreshStats());
        
        assertThat(stringWriter.toString(), containsString("sometype.total"));
        assertThat(stringWriter.toString(), containsString("sometype.totalTimeInMillis"));
        assertThat(stringWriter.toString(), containsString("sometype.waitingListeners"));
        
        assertEquals(stringWriter.toString().split("\n").length, 3);
    }
    
    @Test
    public void testSendCompletionStats() {
        ObjectLongHashMap<String> a1 = new ObjectLongHashMap<>(2);
        a1.put("f1", 1);
        a1.put("f2", 1);
        CompletionStats stats = new CompletionStats(1, new FieldMemoryStats(a1));
        
        reporter.sendCompletionStats("sometype", stats);
        log.debug("Metrics: \n{}", stringWriter);

        assertThat(stringWriter.toString(), containsString("sometype.sizeInBytes"));
        assertThat(stringWriter.toString(), containsString("sometype.fields.f2"));
        assertThat(stringWriter.toString(), containsString("sometype.fields.f1"));
    }
  
        
    @Test
    public void testSendRecoveryStats() {
        RecoveryStats stats = new RecoveryStats();
        reporter.sendRecoveryStats("sometype", stats);
        log.debug("Metrics: \n{}", stringWriter);

        assertThat(stringWriter.toString(), containsString("sometype.throttleTimeInMillis"));
        assertThat(stringWriter.toString(), containsString("sometype.currentAsSource"));
        assertThat(stringWriter.toString(), containsString("sometype.currentAsTarget"));
    }
    
    @Test
    public void sendRequestCacheStats() {
        RequestCacheStats stats = new RequestCacheStats();
        
        reporter.sendRequestCacheStats("sometype", stats);
        log.debug("Metrics: \n{}", stringWriter);
       
        assertThat(stringWriter.toString(), containsString("sometype.evictions"));
        assertThat(stringWriter.toString(), containsString("sometype.hitCount"));
        assertThat(stringWriter.toString(), containsString("sometype.memorySizeInBytes"));
        assertThat(stringWriter.toString(), containsString("sometype.missCount"));
        
    }
    
    @Test
    public void sendTranslogStats() {
        TranslogStats stats = new TranslogStats();
        
        reporter.sendTranslogStats("sometype", stats);
        
        log.debug("Metrics: \n{}", stringWriter);
        
        assertThat(stringWriter.toString(), containsString("sometype.sizeInBytes"));
        assertThat(stringWriter.toString(), containsString("sometype.uncommittedSizeInBytes"));
        assertThat(stringWriter.toString(), containsString("sometype.estimatedNumberOfOperations"));
        assertThat(stringWriter.toString(), containsString("sometype.uncommittedOperations"));
        
    }
    
    @Test
    public void sendNodeWarmerStats() {
        WarmerStats stats = new WarmerStats();
        
        reporter.sendNodeWarmerStats("sometype", stats);
        
        log.debug("Metrics: \n{}", stringWriter);
        
        assertThat(stringWriter.toString(), containsString("sometype.current"));
        assertThat(stringWriter.toString(), containsString("sometype.total"));
        assertThat(stringWriter.toString(), containsString("sometype.totalTimeInMillis"));
    }
    
    @Test
    public void testSendIndexingStats() {
        IndexingStats hack = new IndexingStats();
        IndexingStats stats = new IndexingStats(hack.getTotal(), Collections.singletonMap("idx1", hack.getTotal()));
        
        reporter.sendIndexingStats("sometype", stats);
        log.debug("Metrics: \n{}", stringWriter);
        
        assertThat(stringWriter.toString(), containsString("sometype._all.indexCount"));
        assertThat(stringWriter.toString(), containsString("sometype._all.indexTimeInMillis"));
        assertThat(stringWriter.toString(), containsString("sometype._all.indexCurrent"));
        
        assertThat(stringWriter.toString(), containsString("sometype._all.deleteCount"));
        assertThat(stringWriter.toString(), containsString("sometype._all.deleteTimeInMillis"));
        assertThat(stringWriter.toString(), containsString("sometype._all.deleteCurrent"));
        
        assertThat(stringWriter.toString(), containsString("sometype._all.noopUpdateCount"));
        assertThat(stringWriter.toString(), containsString("sometype._all.throttleTimeInMillis"));
        
        assertThat(stringWriter.toString(), containsString("sometype.idx1.indexCount"));
        assertThat(stringWriter.toString(), containsString("sometype.idx1.indexTimeInMillis"));
        assertThat(stringWriter.toString(), containsString("sometype.idx1.indexCurrent"));
        
        assertThat(stringWriter.toString(), containsString("sometype.idx1.deleteCount"));
        assertThat(stringWriter.toString(), containsString("sometype.idx1.deleteTimeInMillis"));
        assertThat(stringWriter.toString(), containsString("sometype.idx1.deleteCurrent"));
        
        assertThat(stringWriter.toString(), containsString("sometype.idx1.noopUpdateCount"));
        assertThat(stringWriter.toString(), containsString("sometype.idx1.throttleTimeInMillis"));
        
        assertEquals(stringWriter.toString().split("\n").length, 16);
    }

    @Test
    public void testSendGetStats() {
        reporter.sendGetStats("sometype", new GetStats());
        log.debug("Metrics: \n{}", stringWriter);
        
        assertThat(stringWriter.toString(), containsString("sometype.existsCount"));
        assertThat(stringWriter.toString(), containsString("sometype.existsTimeInMillis"));
        assertThat(stringWriter.toString(), containsString("sometype.missingCount"));
        assertThat(stringWriter.toString(), containsString("sometype.missingTimeInMillis"));
        assertThat(stringWriter.toString(), containsString("sometype.current"));
        assertThat(stringWriter.toString(), containsString("sometype.getCount"));
        assertThat(stringWriter.toString(), containsString("sometype.getTimeInMillis"));
        
        assertEquals(stringWriter.toString().split("\n").length, 7);
    }

    @Test
    public void testSendFlushStats() {
        reporter.sendFlushStats("sometype", new FlushStats());
        log.debug("Metrics: \n{}", stringWriter);
        
        assertThat(stringWriter.toString(), containsString("sometype.total"));
        assertThat(stringWriter.toString(), containsString("sometype.totalTimeInMillis"));
        
        assertEquals(stringWriter.toString().split("\n").length, 2);
    }

    @Test
    public void testSendDocsStats() {
        reporter.sendDocsStats("sometype", new DocsStats());
        log.debug("Metrics: \n{}", stringWriter);
        
        assertThat(stringWriter.toString(), containsString("sometype.count"));
        assertThat(stringWriter.toString(), containsString("sometype.deleted"));
        assertThat(stringWriter.toString(), containsString("sometype.averageSizeInBytes"));
        assertThat(stringWriter.toString(), containsString("sometype.totalSizeInBytes"));
        
        assertEquals(stringWriter.toString().split("\n").length, 4);
    }

    @Test
    public void testSendFieldDataStats() {
        ObjectLongHashMap<String> a1 = new ObjectLongHashMap<>(2);
        a1.put("f1", 1);
        a1.put("f2", 1);
        FieldDataStats stats = new FieldDataStats(1, 2, new FieldMemoryStats(a1));

        reporter.sendFieldDataStats("sometype", stats);
        log.debug("Metrics: \n{}", stringWriter);
        
        assertThat(stringWriter.toString(), containsString("sometype.memorySizeInBytes"));
        assertThat(stringWriter.toString(), containsString("sometype.evictions"));
        assertThat(stringWriter.toString(), containsString("sometype.fields.f2"));
        assertThat(stringWriter.toString(), containsString("sometype.fields.f1"));
        assertEquals(stringWriter.toString().split("\n").length, 4);

    }
    
    /**
     * 
     * Simple Code generator.  Helps creating initial assertThat.  Wouldn't normally do it, but this is a lot
     * of boiler plate code.
     * 
     */
    private void assertionGenerator() {
        String[] split = stringWriter.toString().split("\n");
        for (String s : split) {
            if (s.contains("sometype")) {
                s = s.substring(0, s.indexOf(" "));
                System.out.println("assertThat(stringWriter.toString(), containsString(\"" + s + "\"));");
            } else {
                s = s.substring("sometype".length(), s.indexOf(" "));
                System.out.println("assertThat(stringWriter.toString(), containsString(prefix + \"" + s + "\"));");
            }
        }
        System.out.println("assertEquals(stringWriter.toString().split(\"\\n\").length, " + split.length +");");
    }

}


