package net.opentsdb.core;

import java.util.TreeMap;
import java.util.ArrayList;
import static org.hbase.async.Bytes.ByteMap;

public interface StorageQuery {
  
    TreeMap<byte[], Span> findSpans() throws StorageException;
    
    void setMetric(byte[] metric);
          
    void setScanStartTime(long start_time);
          
    void setScanEndTime(long end_time);
          
    void setTags(ArrayList<byte[]> tags);
          
    void setGroupBys(ArrayList<byte[]> group_bys);
          
    void setGroupByValues(ByteMap<byte[][]> group_by_values);
  
}
