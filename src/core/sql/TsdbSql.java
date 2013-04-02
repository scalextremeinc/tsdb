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

public final class TsdbSql implements TSDB {
    
    private static final Logger LOG = LoggerFactory.getLogger(TsdbSql.class);
    
    private DataSource ds;
    
    public TsdbSql(DataSource ds) {
        this.ds = ds;
    }
    
    public Deferred<Object> addPoint(String metric, long timestamp, long value, Map<String, String> tags) {
        LOG.info("SQL add point long");
        try {
            Connection conn = ds.getConnection();
            
        } catch (SQLException e) {
            LOG.error("Unable to get sql db connection: " + e.getMessage());
        }
        return null;
    }
    
    public Deferred<Object> addPoint(String metric, long timestamp, float value, Map<String, String> tags) {
        LOG.info("SQL add point float");
        return null;
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
