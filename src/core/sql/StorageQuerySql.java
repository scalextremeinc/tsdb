package net.opentsdb.core.sql;

import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.HashMap;
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
    private boolean join_tags = false;
    
    private byte[] metric;
    private long start_time;
    private long end_time;
    private ArrayList<byte[]> tags;
    private ArrayList<byte[]> group_bys;
    private ByteMap<byte[][]> group_by_values;
    private boolean rate;
    private Aggregator aggregator;
    private Aggregator downsampler;
    private int sample_interval;
    private Map<byte[], Boolean> plus_aggregate = new HashMap<byte[], Boolean>();
    private Map<String, String> extra_tags;
  
    public StorageQuerySql(TsdbSql tsdb, DataSource ds, String table_tsdb, String table_tsdbtag) {
        this.tsdb = tsdb;
        this.ds = ds;
        this.table_tsdb = table_tsdb;
        this.table_tsdbtag = table_tsdbtag;
    }
    
    public DataPoints[] runQuery() throws StorageException {
        Map<List<Long>, Span> spans = queryDb();
        
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
        for (final Map.Entry<List<Long>, Span> entry : spans.entrySet()) {
            List<Long> key = entry.getKey();
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
    
    private byte[] getTagValue(List<Long> key, byte[] tag_id) {
        Long tag_idl = DataSourceUtil.toLong(tag_id);
        Iterator<Long> i = key.iterator();
        while (i.hasNext()) {
            Long name_id = i.next();
            Long value_id = i.next();
            if (tag_idl.equals(name_id))
                return DataSourceUtil.toBytes(value_id);
        }
        return null;
    }
    
    private String buildQuery() {
        StringBuilder host_condition = new StringBuilder();
        StringBuilder tags_condition = new StringBuilder();
        
        join_tags = buildHostCondition(host_condition);
        join_tags = buildTagsCondition(tags_condition) || join_tags;
        
        boolean group = join_tags && group_bys != null && group_bys.size() > 0;

        StringBuilder query = new StringBuilder("SELECT t.id,t.val_int,t.val_dbl,t.ts,t.hostid");
        if (join_tags)
            query.append(",g.tagkid,g.tagvid");
        query.append(" FROM " + table_tsdb + " as t");
        if (join_tags)
            query.append(", " + table_tsdbtag + " as g");
        query.append(" WHERE t.metricid=");
        query.append(DataSourceUtil.toLong(metric));
        query.append(" AND t.ts >= ");
        query.append(start_time);
        query.append(" AND t.ts <= ");
        query.append(end_time);
        if (join_tags)
            query.append(" AND t.id=g.tsdbid");
        query.append(host_condition);
        
        if (tags_condition.length() > 0 || group)
            query.append(" AND (");
            
        query.append(tags_condition);
        if (group) {
            if (tags_condition.length() > 0)
                query.append(" OR");
            buildGroupingCondition(query);
        }
           
        if (tags_condition.length() > 0 || group)
           query.append(")");
        
        return query.toString();
    }
    
    private boolean buildTagsCondition(StringBuilder tags_condition) {
        boolean join_tags = false;
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
                    tags_condition.append(" OR (");
                }
                System.arraycopy(tag, name_width, value_id, 0, value_width);
                tags_condition.append(" g.tagkid=");
                tags_condition.append(DataSourceUtil.toLong(name_id));
                tags_condition.append(" AND g.tagvid=");
                tags_condition.append(DataSourceUtil.toLong(value_id));
                join_tags = true;
                tags_condition.append(")");
            }
        }
        
        return join_tags;
    }
    
    private void buildGroupingCondition(StringBuilder query) {
        // AND ( g.tagkid=? AND g.tagvid IN (17,18,19,20) OR ... )
        byte[] host_id = getHostId();
        byte[] group_by;
        for (int i = 0; i < group_bys.size(); i++) {
            group_by = group_bys.get(i);
            if (Arrays.equals(group_by, host_id))
                continue;
            if (i > 0)
                query.append(" OR (");
            else
                query.append(" (");
            query.append(" g.tagkid=");
            query.append(DataSourceUtil.toLong(group_by));
            byte[][] value_ids = (group_by_values == null 
                ? null : group_by_values.get(group_by));
            if (value_ids != null && value_ids.length > 0) {
                query.append(" AND g.tagvid IN (");
                for (int j = 0; j <  value_ids.length; j++) {
                    query.append(DataSourceUtil.toLong(value_ids[j]));
                    if (j < value_ids.length - 1)
                        query.append(',');
                }
                query.append(")");
            }
            query.append(")");
        }
    }
    
    private boolean buildHostCondition(StringBuilder host_condition) {
        boolean join_tags = false;
        byte[] host_id = getHostId();

        int name_width = tsdb.getTagNames().width();
        int value_width = tsdb.getTagValues().width();
        byte[] name_id = new byte[name_width];
        byte[] value_id = new byte[value_width];
        for (byte[] tag : tags) {
            System.arraycopy(tag, 0, name_id, 0, name_width);
            if (Arrays.equals(name_id, host_id)) {
                System.arraycopy(tag, name_width, value_id, 0, value_width);
                host_condition.append(" AND t.hostid=");
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
                        host_condition.append(" AND t.hostid IN (");
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
            if (host_condition.length() > 0 && group_bys.size() > 1 
                    || host_condition.length() == 0 && group_bys.size() > 0) {
                join_tags = true;
            }
        } else if (group_bys != null && group_bys.size() > 0) {
            join_tags = true;
        }
        
        return join_tags;
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
    
    private Map<List<Long>, Span> queryDb() {
        String query = buildQuery();
        LOG.info("QUERY: " + query);
        
        Map<List<Long>, Span> spans = new HashMap<List<Long>, Span>();
        Map<List<Long>, SpanViewSql> span_views = new HashMap<List<Long>, SpanViewSql>();
        
        Connection conn = null;
        PreparedStatement st = null;
        ResultSet rs = null;
        try {
            conn = ds.getConnection();
            st = conn.prepareStatement(query);
            //st.setLong(1, DataSourceUtil.toLong(metric));
            rs = st.executeQuery();
            while (rs.next()) {
                updateSpan(spans, span_views, rs);
            }
        } catch (SQLException e) {
            LOG.error("Unable to get results: " + e.getMessage());
        } finally {
            DataSourceUtil.close(rs, st, conn);
        }  
        
        return spans;
    }
    
    private DataPoint current_point;
    private long current_point_id;
    private List<Long> current_key;
    
    private void updateSpan(Map<List<Long>, Span> spans, Map<List<Long>, SpanViewSql> span_views, ResultSet rs)
            throws SQLException {
        
        if (current_key == null) {
            current_key = createKey(rs);
        }
        
        long point_id = rs.getLong(1);
        
        if (current_point == null) {
            current_point = createPoint(rs);
            current_point_id = point_id;
        }
        
        if (current_point_id == point_id && join_tags) {
            current_key.add(rs.getLong(6));
            current_key.add(rs.getLong(7));
        } else if (current_point_id != point_id) {
            SpanViewSql span_view = span_views.get(current_key);
            if (span_view == null) {
                span_view = new SpanViewSql(tsdb.getMetrics().getName(metric));
                // set tags
                Iterator<Long> i = current_key.iterator();
                while (i.hasNext()) {
                    Long name_id = i.next();
                    Long value_id = i.next();
                    span_view.putTag(tsdb.getTagNames().getName(DataSourceUtil.toBytes(name_id)),
                        tsdb.getTagValues().getName(DataSourceUtil.toBytes(value_id)));
                }
                
                //String current_key_str = "";
                //for (Long l : current_key)
                //    current_key_str += "_" + l;
                //LOG.info("new span view: " + span_view + ", key: " + current_key_str);
               
                span_views.put(current_key, span_view);
                List<SpanViewSql> rows = new ArrayList<SpanViewSql>();
                rows.add(span_view);
                Span s = new Span();
                s.setSpanViews(rows);
                spans.put(current_key, s);
            }
            
            span_view.addPoint(current_point);
            
            current_key = createKey(rs);
            current_point = createPoint(rs);
            current_point_id = point_id;
            if (join_tags) {
                current_key.add(rs.getLong(6));
                current_key.add(rs.getLong(7));
            }
        }
    }
    
    private DataPointImpl createPoint(ResultSet rs) throws SQLException {
        double val_dbl = rs.getDouble(3);
        if (!rs.wasNull())
            return new DataPointImpl(rs.getLong(4), val_dbl);
        else
            return new DataPointImpl(rs.getLong(4), rs.getLong(2));
    }
    
    private List<Long> createKey(ResultSet rs) throws SQLException {
        List<Long> key = new LinkedList<Long>();
        long host_value_id = rs.getLong(5);
        if (!rs.wasNull())
            key.add(getHostIdl());
            key.add(host_value_id);
        return key;
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
  
}
