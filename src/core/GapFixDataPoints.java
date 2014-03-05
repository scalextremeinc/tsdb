package net.opentsdb.core;

import java.util.Map;
import java.util.List;
import java.util.NoSuchElementException;

public class GapFixDataPoints implements DataPoints {

    private final DataPoints source;
    private final long interval;
    private final double fixValue;
    private final boolean isInteger;
    private final long startTime;
    private final long endTime;

    public GapFixDataPoints(DataPoints source, long interval, double fixValue,
            boolean isInteger, long startTime, long endTime) {
        this.source = source;
        this.interval = interval;
        this.fixValue = fixValue;
        this.isInteger = isInteger;
        this.startTime = startTime;
        this.endTime = endTime;
    }

    public String metricName() {
        return source.metricName();
    }

    public Map<String, String> getTags() {
        return source.getTags();
    }

    public List<String> getAggregatedTags() {
        return source.getAggregatedTags();
    }

    public SeekableView iterator() {
        return new GapFixIterator(source.iterator(), interval, fixValue, isInteger,
                startTime, endTime);
    }

    public int size() {
        throw new UnsupportedOperationException();
    }

    public int aggregatedSize() {
        throw new UnsupportedOperationException();
    }

    public long timestamp(int i) {
        throw new UnsupportedOperationException();
    }

    public boolean isInteger(int i) {
        throw new UnsupportedOperationException();
    }

    public long longValue(int i) {
        throw new UnsupportedOperationException();
    }

    public double doubleValue(int i) {
        throw new UnsupportedOperationException();
    }
   
    final static class GapFixIterator implements SeekableView, DataPoint {

        private final SeekableView sourceIterator;
        private long lastTimestamp = 0;
        private DataPoint currentPoint;

        private final long interval;
        private final double fixValue;
        private final boolean isInteger;
        private final long startTime;
        private final long endTime;
        
        protected GapFixIterator(SeekableView sourceIterator, long interval, double fixValue,
                boolean isInteger, long startTime, long endTime) {
            this.sourceIterator = sourceIterator;
            this.interval = interval;
            this.fixValue = fixValue;
            this.isInteger = isInteger;
            this.startTime = startTime;
            this.endTime = endTime;
        }

        // Iterator interface //

        public boolean hasNext() {
            return sourceIterator.hasNext() || lastTimestamp + interval <= endTime;
        }

        public DataPoint next() {
            if (!hasNext()) {
                throw new NoSuchElementException("no more elements");
            }

            if (0 == lastTimestamp) {
                currentPoint = sourceIterator.next();
                if (startTime + interval > currentPoint.timestamp()) {
                    lastTimestamp = currentPoint.timestamp();
                    return currentPoint;
                } else {
                    lastTimestamp = startTime - (startTime % interval);
                    return this;
                }
            }
            
            boolean newPoint = false;
            if (sourceIterator.hasNext() && lastTimestamp >= currentPoint.timestamp()) {
                currentPoint = sourceIterator.next();
                newPoint = true;
            }

            if (lastTimestamp + interval < currentPoint.timestamp() ||
                    (!newPoint && lastTimestamp + interval <= endTime)) {
                // fill the gap by returning this
                lastTimestamp = lastTimestamp + interval;
                return this;
            }

            lastTimestamp = currentPoint.timestamp();

            return currentPoint;
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }

        // SeekableView interface //

        public void seek(final long timestamp) {
            sourceIterator.seek(timestamp);
        }

        // DataPoint interface //

        public long timestamp() {
            return lastTimestamp;
        }

        public boolean isInteger() {
            return isInteger;
        }

        public long longValue() {
            return (long) fixValue;
        }

        public double doubleValue() {
            return fixValue;
        }

        public double toDouble() {
            return doubleValue();
        }

    }

}
