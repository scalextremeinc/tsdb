package net.opentsdb.core.sql;

import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.hbase.async.Bytes.ByteMap;
import net.opentsdb.core.StorageQuery;
import net.opentsdb.core.StorageException;
import net.opentsdb.core.Span;
import net.opentsdb.core.SpanGroup;
import net.opentsdb.core.DataPoint;
import net.opentsdb.core.DataPointImpl;
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.Aggregator;
import net.opentsdb.core.RowKey;
import net.opentsdb.core.GapFixSpan;
import net.opentsdb.core.SpanCmp;
import net.opentsdb.core.EmptySpanUtil;

public class StorageQuerySql implements StorageQuery {
    
    /** Used whenever there are no results. */
    private static final DataPoints[] NO_RESULT = new DataPoints[0];
    
    private static final Logger LOG = LoggerFactory.getLogger(StorageQuerySql.class);
    
    private final TsdbSql tsdb;
    private DataSource ds;
    private final String table_tsdb;
    private final String table_tsdbtag;
    private byte[] host_name_id;
    private Long host_name_idl;
    private String[] tags_columns = {"hostid", "t0_valueid", "t1_valueid",
        "t2_valueid", "t3_valueid", "t4_valueid", "t5_valueid", "t6_valueid"};
    
    private byte[] metric;
    private String metricName;
    private long start_time;
    private long end_time;
    private ArrayList<byte[]> tags;
    private ArrayList<byte[]> empty_tags;
    private ArrayList<byte[]> group_bys;
    private ByteMap<byte[][]> group_by_values;
    private boolean rate;
    private Aggregator aggregator;
    private Aggregator downsampler;
    private int sample_interval;
    private Map<byte[], Boolean> plus_aggregate = new HashMap<byte[], Boolean>();
    private Map<String, String> extra_tags;

    private Boolean isAvail;
    private Long availInterval;

    private short metric_width;
    private short name_width;
    private short value_width;
  
    public StorageQuerySql(TsdbSql tsdb, DataSource ds, String table_tsdb, String table_tsdbtag) {
        this.tsdb = tsdb;
        this.ds = ds;
        this.table_tsdb = table_tsdb;
        this.table_tsdbtag = table_tsdbtag;
        this.metric_width = tsdb.getMetrics().width();
        this.name_width = tsdb.getTagNames().width();
        this.value_width = tsdb.getTagValues().width();
    }
    
    public DataPoints[] runQuery() throws StorageException {
        Map<byte[], Span> spans = queryDb();
        
        if (spans == null || spans.size() <= 0) {
          return NO_RESULT;
        }
        
        if (group_bys == null) {
          // We haven't been asked to find groups, so let's put all the spans
          // together in the same group.
          final SpanGroup group = new SpanGroup(tsdb, start_time, end_time,
            spans.values(), rate, aggregator, sample_interval, downsampler);
          return new SpanGroup[] { group };
        }

        final ByteMap<SpanGroup> groups = new ByteMap<SpanGroup>();
        final short value_width = tsdb.getTagValues().width();
        final byte[] group = new byte[group_bys.size() * value_width];
        for (final Map.Entry<byte[], Span> entry : spans.entrySet()) {
            byte[] key = entry.getKey();
            byte[] value_id = null;
            int i = 0;
            for (final byte[] tag_id : group_bys) {
                Boolean is_plus_aggregate = plus_aggregate.get(tag_id);
                if (is_plus_aggregate != null && is_plus_aggregate) {
                    value_id = tag_id;
                } else {
                    value_id = getTagValue(key, tag_id);
                }
                if (value_id == null) {
                  break;
                }
                System.arraycopy(value_id, 0, group, i, value_width);
                i += value_width;
            }
            SpanGroup thegroup = groups.get(group);
            if (thegroup == null) {
                thegroup = new SpanGroup(tsdb, start_time, end_time,
                    null, rate, aggregator, sample_interval, downsampler);
                thegroup.setExtraTags(extra_tags);
                // Copy the array because we're going to keep `group' and overwrite
                // its contents.  So we want the collection to have an immutable copy.
                final byte[] group_copy = new byte[group.length];
                System.arraycopy(group, 0, group_copy, 0, group.length);
                groups.put(group_copy, thegroup);
            }
            thegroup.add(entry.getValue());
        }
        
        return groups.values().toArray(new SpanGroup[groups.size()]);
    }
    
    private byte[] getTagValue(byte[] key, byte[] tag_id) {
        Iterator<byte[]> i = new RowKey.TagIterator(tsdb, key);
        while (i.hasNext()) {
            byte[] name = i.next();
            byte[] value = i.next();
            if (Arrays.equals(tag_id, name))
                return value;
        }
        return null;
    }
    
    private String buildQuery() {
        StringBuilder host_condition = new StringBuilder();
        StringBuilder tags_condition = new StringBuilder();
        StringBuilder group_condition = new StringBuilder();
        
        buildHostCondition(host_condition);
        buildTagsCondition(tags_condition);
        buildGroupingCondition(group_condition);

        StringBuilder query = new StringBuilder("SELECT val_int,val_dbl,ts");
        for (String col : tags_columns) {
            query.append(',');
            query.append(col);
        }
        query.append(" FROM ");
        query.append(table_tsdb);
        query.append(" WHERE metricid=");
        query.append(DataSourceUtil.toLong(metric));
        query.append(" AND ts >= ");
        query.append(start_time);
        query.append(" AND ts <= ");
        query.append(end_time);
        query.append(host_condition);
        
        if (tags_condition.length() > 0 || group_condition.length() > 0) {
            query.append(" AND (");
        }
            
        query.append(tags_condition);
        if (group_condition.length() > 0) {
            if (tags_condition.length() > 0)
                query.append(" AND");
            query.append(group_condition);
        }
           
        if (tags_condition.length() > 0 || group_condition.length() > 0)
           query.append(")");
        
        query.append(" ORDER BY ts");
        
        return query.toString();
    }
    
    private void buildHostCondition(StringBuilder host_condition) {
        byte[] host_id = getHostId();

        int name_width = tsdb.getTagNames().width();
        int value_width = tsdb.getTagValues().width();
        byte[] name_id = new byte[name_width];
        byte[] value_id = new byte[value_width];
        for (byte[] tag : tags) {
            System.arraycopy(tag, 0, name_id, 0, name_width);
            if (Arrays.equals(name_id, host_id)) {
                System.arraycopy(tag, name_width, value_id, 0, value_width);
                host_condition.append(" AND hostid=");
                host_condition.append(DataSourceUtil.toLong(value_id));
                break;
            }
        }
        
        //LOG.info("group_bys size: " + group_bys.size());
        if (host_condition.length() == 0 && group_bys != null && group_bys.size() > 0) {
            byte[] group_by;
            for (int i = 0; i < group_bys.size(); i++) {
                group_by = group_bys.get(i);
                if (Arrays.equals(group_by, host_id)) {
                    byte[][] value_ids = (group_by_values == null 
                        ? null : group_by_values.get(group_by));
                    if (value_ids != null && value_ids.length > 0) {
                        host_condition.append(" AND hostid IN (");
                        for (int j = 0; j <  value_ids.length; j++) {
                            host_condition.append(DataSourceUtil.toLong(value_ids[j]));
                            if (j < value_ids.length - 1)
                                host_condition.append(',');
                        }
                        host_condition.append(")");
                        
                    }
                    break;
                }
            }
        }
    }
    
    private void buildTagsCondition(StringBuilder tags_condition) {
        byte[] host_id = getHostId();

        int name_width = tsdb.getTagNames().width();
        int value_width = tsdb.getTagValues().width();
        byte[] name_id = new byte[name_width];
        byte[] value_id = new byte[value_width];
        boolean empty = true;
        for (byte[] tag : tags) {
            System.arraycopy(tag, 0, name_id, 0, name_width);
            if (!Arrays.equals(name_id, host_id)) {
                if (empty) {
                    tags_condition.append(" (");
                    empty = false;
                } else {
                    tags_condition.append(" AND (");
                }
                
                String tag_name = tsdb.getTagNames().getName(name_id);
                String column_name = tag_name + "_valueid";
                
                System.arraycopy(tag, name_width, value_id, 0, value_width);
                tags_condition.append(' ');
                tags_condition.append(column_name);
                tags_condition.append('=');
                tags_condition.append(DataSourceUtil.toLong(value_id));
                tags_condition.append(")");                
            }
        }
        
        for (byte[] empty_tag : empty_tags) {
            String tag_name = tsdb.getTagNames().getName(empty_tag);
            String column_name = tag_name + "_valueid";
            if (empty) {
                tags_condition.append(" (");
                empty = false;
            } else {
                tags_condition.append(" AND (");
            }
            tags_condition.append(column_name);
            tags_condition.append(" is NULL)");
        }
    }
    
    private void buildGroupingCondition(StringBuilder group_condition) {
        if (group_bys == null)
            return;
        // AND ( g.tagkid=? AND g.tagvid IN (17,18,19,20) OR ... )
        byte[] host_id = getHostId();
        byte[] group_by;
        for (int i = 0; i < group_bys.size(); i++) {
            group_by = group_bys.get(i);
            if (Arrays.equals(group_by, host_id))
                continue;
                            
            String tag_name = tsdb.getTagNames().getName(group_by);
            String column_name = tag_name + "_valueid";
            
            byte[][] value_ids = (group_by_values == null ? null : group_by_values.get(group_by));
            if (value_ids != null && value_ids.length > 0) {
                if (i > 0)
                    group_condition.append(" AND (");
                else
                    group_condition.append(" (");
                group_condition.append(' ');
                group_condition.append(column_name);
                group_condition.append(" IN (");
                for (int j = 0; j <  value_ids.length; j++) {
                    group_condition.append(DataSourceUtil.toLong(value_ids[j]));
                    if (j < value_ids.length - 1)
                        group_condition.append(',');
                }
                group_condition.append(")");
                group_condition.append(")");
            }
        }
    }
    
    private byte[] getHostId() {
        if (host_name_id == null) {
            host_name_id = tsdb.getTagNames().getId("host");
        }
        return host_name_id;
    }
    
    private long getHostIdl() {
        if (host_name_idl == null) {
            host_name_idl = DataSourceUtil.toLong(getHostId());
        }
        return host_name_idl;
    }
    
    private Map<byte[], Span> queryDb() {
        String query = buildQuery();
        LOG.info(query);
        
        TreeMap<byte[], Span> spans = new TreeMap<byte[], Span>(new SpanCmp(metric_width));
        TreeMap<byte[], SpanViewSql> span_views = new TreeMap<byte[], SpanViewSql>(new SpanCmp(metric_width));
        
        Connection conn = null;
        PreparedStatement st = null;
        ResultSet rs = null;
        try {
            conn = ds.getConnection();
            st = conn.prepareStatement(query);
            rs = st.executeQuery();
            while (rs.next()) {
                updateSpan(spans, span_views, rs);
            }
        } catch (SQLException e) {
            LOG.error("Unable to get results: " + e.getMessage());
        } finally {
            DataSourceUtil.close(rs, st, conn);
        }  

        if (isAvail) {
            EmptySpanUtil.insertEmptySpans(spans, tsdb, availInterval, start_time, end_time,
                    metric, metricName, tags, group_bys, group_by_values);
        }
        
        return spans;
    }
    
    private void updateSpan(Map<byte[], Span> spans, Map<byte[], SpanViewSql> span_views, ResultSet rs)
            throws SQLException {
        
        List<byte[]> tag_kvs = createTagKVs(rs);
        byte[] key = RowKey.createRowKey(tsdb, metric, tag_kvs);

        DataPoint point = createPoint(rs);

        SpanViewSql span_view = span_views.get(key);
        if (span_view == null) {
            span_view = new SpanViewSql(tsdb.getMetrics().getName(metric));
            // set tags
            Iterator<byte[]> i = new RowKey.TagIterator(tsdb, key);
            while (i.hasNext()) {
                byte[] name_id = i.next();
                byte[] value_id = i.next();;
                // add tag to span view only if it appears in tags or groupbys
                boolean add = false;
                if (group_bys != null)
                    for (final byte[] tag_id : group_bys)
                        if (Arrays.equals(tag_id, name_id))
                            add = true;
                if (!add) {
                    int name_width = tsdb.getTagNames().width();
                    byte[] tag_id = new byte[name_width];
                    for (byte[] tag : tags) {
                        System.arraycopy(tag, 0, tag_id, 0, name_width);
                        if (Arrays.equals(tag_id, name_id))
                            add = true;
                    }
                }
                if (add)
                    span_view.putTag(tsdb.getTagNames().getName(name_id), tsdb.getTagValues().getName(value_id));
            }
            
            span_views.put(key, span_view);
            List<SpanViewSql> rows = new ArrayList<SpanViewSql>();
            rows.add(span_view);
            Span s = null;
            if (isAvail) {
              LOG.info("AVAILABILITY: initializing span gap fixer, interval: " + availInterval);
              s = new GapFixSpan(availInterval, 0.0, false, start_time, end_time); 
            } else {
              s = new Span();
            }
            s.setSpanViews(rows);
            spans.put(key, s);
        }
            
        span_view.addPoint(point);
    }
    
    private DataPointImpl createPoint(ResultSet rs) throws SQLException {
        double val_dbl = rs.getDouble(2);
        if (!rs.wasNull())
            return new DataPointImpl(rs.getLong(3), val_dbl);
        else
            return new DataPointImpl(rs.getLong(3), rs.getLong(1));
    }

    private List<byte[]> createTagKVs(ResultSet rs) throws SQLException {
        ArrayList<byte[]> tag_kvs = new ArrayList<byte[]>();
        for (String tagcol : tags_columns) {
            if ("hostid".equals(tagcol)) {
                long host_value_id = rs.getLong(4);
                if (!rs.wasNull()) {
                    byte[] tag_kv = new byte[name_width + value_width];
                    System.arraycopy(getHostId(), 0, tag_kv, 0, name_width);
                    System.arraycopy(DataSourceUtil.toBytes(host_value_id), 0, tag_kv, name_width, value_width);
                    tag_kvs.add(tag_kv);
                }
            } else {
                long value_id = rs.getLong(tagcol);
                if (!rs.wasNull()) {
                    String tag = tagcol.substring(0, 2);
                    byte[] tag_kv = new byte[name_width + value_width];
                    System.arraycopy(tsdb.getTagNames().getId(tag), 0, tag_kv, 0, name_width);
                    System.arraycopy(DataSourceUtil.toBytes(value_id), 0, tag_kv, name_width, value_width);
                    tag_kvs.add(tag_kv);
                }
            }
        }

        return tag_kvs;
    }

    
    public void setMetric(byte[] metric) {
        this.metric = metric;
    }
          
    public void setScanStartTime(long start_time) {
        this.start_time = start_time;
    }
          
    public void setScanEndTime(long end_time) {
        this.end_time = end_time;
    }
          
    public void setTags(ArrayList<byte[]> tags) {
        this.tags = tags;
    }
    
    public void setEmptyTags(ArrayList<byte[]> empty_tags) {
        this.empty_tags = empty_tags;
    }
          
    public void setGroupBys(ArrayList<byte[]> group_bys) {
        this.group_bys = group_bys;
    }
          
    public void setGroupByValues(ByteMap<byte[][]> group_by_values) {
        this.group_by_values = group_by_values;
    }
    
    public void setRate(boolean rate) {
      this.rate = rate;
    }

    public void serAggregator(Aggregator aggregator) {
      this.aggregator = aggregator;
    }

    public void setDownsampler(Aggregator downsampler) {
      this.downsampler = downsampler;
    }

    public void setSampleInterval(int sample_interval) {
      this.sample_interval = sample_interval;
    }

    public void setPlusAggregate(Map<byte[], Boolean> plus_aggregate) {
      this.plus_aggregate = plus_aggregate;
    }

    public void setExtraTags(Map<String, String> extra_tags) {
      this.extra_tags = extra_tags;
    }

    public void setIsAvail(Boolean isAvail) {
        this.isAvail = isAvail;
    }

    public void setAvailInterval(Long availInterval) {
        this.availInterval = availInterval;
    }

    public void setMetricName(String metricName) {
        this.metricName = metricName;
    }

}
