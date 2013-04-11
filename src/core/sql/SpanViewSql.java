package net.opentsdb.core.sql;

import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Collections;

import net.opentsdb.core.SpanView;
import net.opentsdb.core.SpanViewIterator;
import net.opentsdb.core.SeekableView;
import net.opentsdb.core.DataPoint;

public class SpanViewSql implements SpanView {
    
    private ArrayList<DataPoint> points = new ArrayList<DataPoint>();
    private String metricName;
    private Map<String, String> tags = new HashMap<String, String>();
    
    public SpanViewSql(String metricName) {
        this.metricName = metricName;
    }
    
    public void addPoint(DataPoint point) {
        points.add(point);
    }
    
    public void putTag(String name, String value) {
        tags.put(name, value);
    }
    
    public boolean hasTags() {
        return !tags.isEmpty();
    }
    
    public String metricName() {
        return metricName;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public List<String> getAggregatedTags() {
        return Collections.emptyList();
    }

    public int size() {
        return points.size();
    }

    public int aggregatedSize() {
        return 0;
    }

    public long timestamp(int i) {
        return points.get(i).timestamp();
    }

    public boolean isInteger(int i) {
        return points.get(i).isInteger();
    }

    public long longValue(int i) {
        return points.get(i).longValue();
    }

    public double doubleValue(int i) {
        return points.get(i).doubleValue();
    }
    
    public SpanViewIterator internalIterator() {
        return new Iterator();
    }
    
    public SeekableView iterator() {
        return internalIterator();
    }
    
    final class Iterator implements SpanViewIterator {
        
        private int index = 0;
        
        /* SpanViewIterator */
        
        public int saveState() {
            return index;
        }

        public void restoreState(int state) {
            index = state;
        }

        public long peekNextTimestamp() {
            return points.get(index + 1).timestamp();
        }
        

        public String toStringSummary() {
            return "SpanViewSql.Iterator(index=" + index + ")";
        }
        
        /* SeekableView */
        
        public boolean hasNext() {
            return index < points.size();
        }

        public DataPoint next() {
            return points.get(index++);
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }

        public void seek(long timestamp) {
            while (index < (points.size() - 1) && peekNextTimestamp() < timestamp) {
                index++;
            }
        }
        
        /* DataPoint */
        
        public long timestamp() {
            return points.get(index).timestamp();
        }

        public boolean isInteger() {
            return points.get(index).isInteger();
        }

        public long longValue() {
            return points.get(index).longValue();
        }

        public double doubleValue() {
            return points.get(index).doubleValue();
        }

        public double toDouble() {
            return points.get(index).toDouble();
        }
        
    }
  
}
