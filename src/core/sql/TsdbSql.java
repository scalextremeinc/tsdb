package net.opentsdb.core.sql;

import java.util.List;
import java.util.Map;

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

import net.opentsdb.uid.sql.UniqueIdSql;

public final class TsdbSql implements TSDB {
    
    private static final Logger LOG = LoggerFactory.getLogger(TsdbSql.class);
    
    private DataSource ds;
    
    private final UniqueIdSql metrics;
    private final UniqueIdSql tag_names;
    private final UniqueIdSql tag_values;
    
    private final String table_tsdb;
    private final String table_tsdbtag;
    
    private final String insert_int_query;
    private final String insert_double_query;
    private final String insert_tag_query;
    
    public TsdbSql(DataSource ds, String table_prefix) {
        this.ds = ds;
        metrics = new UniqueIdSql(ds, addPrefix(table_prefix, "metric"));
        tag_names = new UniqueIdSql(ds, addPrefix(table_prefix, "tagk"));
        tag_values = new UniqueIdSql(ds, addPrefix(table_prefix, "tagv"));
        table_tsdb = addPrefix(table_prefix, "tsdb");
        table_tsdbtag = addPrefix(table_prefix, "tsdbtag");
        insert_int_query = "INSERT INTO " + table_tsdb + " (val_int, ts, metricid, hostid) VALUES (?, ?, ?, ?)";
        insert_double_query = "INSERT INTO " + table_tsdb + " (val_dbl, ts, metricid, hostid) VALUES (?, ?, ?, ?)";
        insert_tag_query = "INSERT INTO " + table_tsdbtag + " VALUES (?, ?, ?)";
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
    
    private void addPoint(String metric, long timestamp, Map<String, String> tags,
            Float val_dbl, Long val_int) {
        byte[] metric_id = metrics.getOrCreateId(metric);
        String host = tags.get("host");
        byte[] host_id = null;
        if (host != null) {
            host_id = tag_values.getOrCreateId(host);
            // make sure host name tag id is created - used by query
            tag_names.getOrCreateId("host");
        }
        
        String insert_query = insert_double_query;
        if (val_int != null) {
            insert_query = insert_int_query;
        }
        
        Connection conn = null;
        PreparedStatement st = null;
        ResultSet rs = null;
        try {
            conn = ds.getConnection();
            conn.setAutoCommit(false);
            long id;
            try {
                st = conn.prepareStatement(insert_query, Statement.RETURN_GENERATED_KEYS);
                if (val_dbl != null)
                    st.setDouble(1, val_dbl);
                else
                    st.setLong(1, val_int);
                st.setLong(2, timestamp);
                st.setLong(3, DataSourceUtil.toLong(metric_id));
                if (host_id != null) {
                    st.setLong(4, DataSourceUtil.toLong(host_id));
                } else
                    st.setNull(4, Types.INTEGER);
                st.executeUpdate();
                
                rs = st.getGeneratedKeys();
                rs.next();
                id = rs.getLong(1);
            } finally {
                DataSourceUtil.close(rs, st, null);
            }
            byte[] tagk = null;
            byte[] tagv = null;
            for (Map.Entry<String, String> entry : tags.entrySet()) {
                if ("host".equals(entry.getKey())) {
                    continue;
                }
                tagk = tag_names.getOrCreateId(entry.getKey());
                tagv = tag_values.getOrCreateId(entry.getValue());
                if (tagk != null && tagv != null)
                    try {
                        st = conn.prepareStatement(insert_tag_query);
                        st.setLong(1, id);
                        st.setLong(2, DataSourceUtil.toLong(tagk));
                        st.setLong(3, DataSourceUtil.toLong(tagv));
                        st.executeUpdate();
                    } finally {
                        st.close();
                    }
            }
            conn.commit();
        } catch (SQLException e) {
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
        query.setStorageQuery(new StorageQuerySql(this, ds, table_tsdb, table_tsdbtag));
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
