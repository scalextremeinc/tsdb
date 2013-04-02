package net.opentsdb.core.sql;

import java.util.List;
import java.util.Map;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stumbleupon.async.Deferred;

import net.opentsdb.uid.UniqueIdInterface;
import net.opentsdb.stats.Histogram;
import net.opentsdb.stats.StatsCollector;

import net.opentsdb.core.TSDB;
import net.opentsdb.core.Query;
import net.opentsdb.core.WritableDataPoints;

import net.opentsdb.uid.sql.UniqueIdSql;

public final class TsdbSql implements TSDB {
    
    private static final Logger LOG = LoggerFactory.getLogger(TsdbSql.class);
    
    private DataSource ds;
    
    final public UniqueIdSql metrics;
    final public UniqueIdSql tag_names;
    final public UniqueIdSql tag_values;
    final public UniqueIdSql hosts;
    
    public TsdbSql(DataSource ds) {
        this.ds = ds;
        metrics = new UniqueIdSql(ds, "metric");
        tag_names = new UniqueIdSql(ds, "tagk");
        tag_values = new UniqueIdSql(ds, "tagv");
        hosts = new UniqueIdSql(ds, "host");
    }
    
    public Deferred<Object> addPoint(String metric, long timestamp, long value, Map<String, String> tags) {
        LOG.info("SQL add point long");
        //try {
            //Connection conn = ds.getConnection();
            
            byte[] id = metrics.getOrCreateId(metric);
            
            LOG.info("METRIC ID: " + id);
            
        //} catch (SQLException e) {
        //    LOG.error("Unable to get sql db connection: " + e.getMessage());
        //}
        return new Deferred<Object>();
    }
    
    public Deferred<Object> addPoint(String metric, long timestamp, float value, Map<String, String> tags) {
        LOG.info("SQL add point float");
        return new Deferred<Object>();
    }
    
    public UniqueIdInterface getMetrics() {
        return null;
    }

    public UniqueIdInterface getTagNames() {
        return null;
    }

    public UniqueIdInterface getTagValues() {
        return null;
    }

    public int uidCacheHits() {
        return 0;
    }

    public int uidCacheMisses() {
        return 0;
    }

    public int uidCacheSize() {
        return 0;
    }

    public void collectStats(final StatsCollector collector) {
        
    }

    public Histogram getPutLatencyHistogram() {
        return null;
    }

    public Histogram getScanLatencyHistogram() {
        return null;
    }

    public Query newQuery() {
        return null;
    }

    public WritableDataPoints newDataPoints() {
        return null;
    }

    public Deferred<Object> flush() throws Exception {
        return null;
    }

    public Deferred<Object> shutdown() {
        return null;
    }

    public List<String> suggestMetrics(final String search) {
        return null;
    }

    public List<String> suggestTagNames(final String search) {
        return null;
    }

    public List<String> suggestTagValues(final String search) {
        return null;
    }
    
}
