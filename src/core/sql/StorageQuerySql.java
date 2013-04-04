package net.opentsdb.core.sql;

import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Arrays;

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
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.Aggregator;

public class StorageQuerySql implements StorageQuery {
    
    private static final Logger LOG = LoggerFactory.getLogger(StorageQuerySql.class);
    
    private final TsdbSql tsdb;
    private DataSource ds;
    private final String table_tsdb;
    private final String table_tsdbtag;
    private final String query_base;
    
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
        query_base = "SELECT t.val_int,t.val_dbl,t.ts, g.tagkid,g.tagvid "
            + "FROM " + table_tsdb + " as t, " + table_tsdbtag + " as g WHERE metricid=?";
    }
    
    public DataPoints[] runQuery() throws StorageException {
        queryDb();
        return null;
    }
    
    private String buildQuery() {
        StringBuilder host_condition = new StringBuilder();
        StringBuilder tags_condition = new StringBuilder();
        
        boolean join_tags = buildHostCondition(host_condition);
        join_tags = buildTagsCondition(tags_condition) || join_tags;

        StringBuilder query = new StringBuilder("SELECT t.val_int,t.val_dbl,t.ts,t.hostid");
        if (join_tags)
            query.append(",g.tagkid,g.tagvid");
        query.append(" FROM " + table_tsdb + " as t");
        if (join_tags)
            query.append(", " + table_tsdbtag + " as g");
        query.append(" WHERE metricid=");
        query.append(DataSourceUtil.toLong(metric));
        if (join_tags)
            query.append(" AND t.id=g.tsdbid");
        query.append(host_condition);
        query.append(tags_condition);
        
        if (join_tags && group_bys != null && group_bys.size() > 0)
           buildGroupingCondition(query);
        
        return query.toString();
    }
    
    private boolean buildTagsCondition(StringBuilder tags_condition) {
        boolean join_tags = false;
        byte[] host_id = getHostId();

        int name_width = tsdb.getTagNames().width();
        int value_width = tsdb.getTagValues().width();
        byte[] name_id = new byte[name_width];
        byte[] value_id = new byte[value_width];
        for (byte[] tag : tags) {
            System.arraycopy(tag, 0, name_id, 0, name_width);
            if (!Arrays.equals(name_id, host_id)) {
                System.arraycopy(tag, name_width, value_id, 0, value_width);
                tags_condition.append(" AND g.tagkid=");
                tags_condition.append(DataSourceUtil.toLong(name_id));
                tags_condition.append(" AND g.tagvid=");
                tags_condition.append(DataSourceUtil.toLong(value_id));
                join_tags = true;
            }
        }
        
        return join_tags;
    }
    
    private void buildGroupingCondition(StringBuilder query) {
        // AND ( g.tagkid=? AND g.tagvid IN (17,18,19,20) OR ... )
        byte[] host_id = getHostId();
        byte[] group_by;
        query.append(" AND (");
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
        query.append(")");
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
        return tsdb.getTagNames().getId("host");
    }
    
    private void queryDb() {
        Connection conn = null;
        PreparedStatement st = null;
        ResultSet rs = null;
        String query = buildQuery();
        LOG.info("QUERY: " + query);
        try {
            conn = ds.getConnection();
            st = conn.prepareStatement(query);
            //st.setLong(1, DataSourceUtil.toLong(metric));
            rs = st.executeQuery();
            
        } catch (SQLException e) {
            LOG.error("Unable to get results: " + e.getMessage());
        } finally {
            DataSourceUtil.close(rs, st, conn);
        }  
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
