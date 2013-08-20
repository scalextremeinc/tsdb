package net.opentsdb.core;

import java.util.Map;
import java.util.ArrayList;
import static org.hbase.async.Bytes.ByteMap;

public interface StorageQuery {
  
    DataPoints[] runQuery() throws StorageException;
    
    void setMetric(byte[] metric);
    void setScanStartTime(long start_time);
    void setScanEndTime(long end_time);
    void setTags(ArrayList<byte[]> tags);
    void setEmptyTags(ArrayList<byte[]> empty_tags);
    void setGroupBys(ArrayList<byte[]> group_bys);
    void setGroupByValues(ByteMap<byte[][]> group_by_values);
    void setRate(boolean rate);
    void serAggregator(Aggregator aggregator);
    void setDownsampler(Aggregator downsampler);
    void setSampleInterval(int sample_interval);
    void setPlusAggregate(Map<byte[], Boolean> plus_aggregate);
    void setExtraTags(Map<String, String> extra_tags);
  
}
