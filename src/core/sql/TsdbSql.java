package net.opentsdb.core.sql;

import java.util.List;
import java.util.Map;
import java.util.Set;

import java.nio.ByteBuffer;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stumbleupon.async.Deferred;

import net.opentsdb.uid.UniqueIdInterface;
import net.opentsdb.stats.Histogram;
import net.opentsdb.stats.StatsCollector;

import net.opentsdb.core.TSDB;
import net.opentsdb.core.Query;
import net.opentsdb.core.TsdbQuery;
import net.opentsdb.core.WritableDataPoints;
import net.opentsdb.core.IncomingDataPoints;

import net.opentsdb.uid.sql.UniqueIdSql;

public final class TsdbSql implements TSDB {
    
    private static final Logger LOG = LoggerFactory.getLogger(TsdbSql.class);
    
    private DataSource ds;
    
    private final UniqueIdSql metrics;
    private final UniqueIdSql tag_names;
    private final UniqueIdSql tag_values;
    
    private final String table_tsdb;
    
    public TsdbSql(DataSource ds, String table_prefix) {
        this.ds = ds;
        metrics = new UniqueIdSql(ds, addPrefix(table_prefix, "metric"));
        tag_names = new UniqueIdSql(ds, addPrefix(table_prefix, "tagk"));
        tag_values = new UniqueIdSql(ds, addPrefix(table_prefix, "tagv"));
        table_tsdb = addPrefix(table_prefix, "tsdb");
    }
    
    private String addPrefix(String prefix, String name) {
        if (prefix != null) {
            return prefix + name;
        }
        return name;
    }
    
    public Deferred<Object> addPoint(String metric, long timestamp,
            long value, Map<String, String> tags) {
        addPoint(metric, timestamp, tags, null, new Long(value));
        return new Deferred<Object>();
    }
    
    public Deferred<Object> addPoint(String metric, long timestamp, float value, Map<String, String> tags) {
        addPoint(metric, timestamp, tags, new Float(value), null);
        return new Deferred<Object>();
    }
    
    private String buildInsertQuery(String metric, long timestamp, Map<String, String> tags,
            Float val_dbl, Long val_int) {
        
        byte[] metric_id = metrics.getOrCreateId(metric);
        String host = tags.get("host");
        byte[] host_id = null;
        if (host != null) {
            host_id = tag_values.getOrCreateId(host);
            // make sure host name tag id is created - used by query
            tag_names.getOrCreateId("host");
        }
        
        StringBuilder query = new StringBuilder();
        query.append("INSERT INTO ");
        query.append(table_tsdb);
        if (val_dbl != null)
            query.append("(val_dbl,");
        else
            query.append("(val_int,");
        query.append("ts,metricid");
        if (host_id != null)
            query.append(",hostid");
        Set<String> tag_keys = tags.keySet();
        for (String tag : tag_keys) {
            if ("host".equals(tag))
                continue;
            query.append(',');
            query.append(tag);
            query.append("_valueid");
            // make sure tag name id is created - used by query
            tag_names.getOrCreateId(tag);
        }
        query.append(") VALUES (");
        if (val_dbl != null)
            query.append(val_dbl);
        else
            query.append(val_int);
        query.append(',');
        query.append(timestamp);
        query.append(',');
        query.append(DataSourceUtil.toLong(metric_id));
        if (host_id != null) {
            query.append(',');
            query.append(DataSourceUtil.toLong(host_id));
        }
        // append tags
        for (String tag : tag_keys) {
            if ("host".equals(tag))
                continue;
            String value = tags.get(tag);
            byte[] value_id = tag_values.getOrCreateId(value);
            query.append(',');
            query.append(DataSourceUtil.toLong(value_id));
        }
        query.append(')');
        
        return query.toString();
    }
    
    private void addPoint(String metric, long timestamp, Map<String, String> tags,
            Float val_dbl, Long val_int) {

        /*if ((timestamp & 0xFFFFFFFF00000000L) != 0) {
          // => timestamp < 0 || timestamp > Integer.MAX_VALUE
          throw new IllegalArgumentException((timestamp < 0 ? "negative " : "bad")
              + " timestamp=" + timestamp
              + " when trying to add value=" + Arrays.toString(value) + '/' + flags
              + " to metric=" + metric + ", tags=" + tags);
        }
        */
        IncomingDataPoints.checkMetricAndTags(metric, tags);
        
        String insert_query = buildInsertQuery(metric, timestamp, tags, val_dbl, val_int);
        //LOG.info(insert_query);
        
        Connection conn = null;
        PreparedStatement st = null;
        ResultSet rs = null;
        try {
            conn = ds.getConnection();
            try {
                st = conn.prepareStatement(insert_query);
                st.executeUpdate();
            } finally {
                DataSourceUtil.close(rs, st, null);
            }
        } catch (SQLException e) {
            LOG.info(insert_query);
            LOG.error("Unable to get sql db connection: " + e.getMessage());
        } finally {
            DataSourceUtil.close(rs, st, conn);
        }
    }
    
    public UniqueIdInterface getMetrics() {
        return metrics;
    }

    public UniqueIdInterface getTagNames() {
        return tag_names;
    }

    public UniqueIdInterface getTagValues() {
        return tag_values;
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
        TsdbQuery query = new TsdbQuery(this);
        query.setStorageQuery(new StorageQuerySql(this, ds, table_tsdb, null));
        return query;
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
