package net.opentsdb.core;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Collections;

public class EmptySpan extends Span {

    private final long interval;
    private final double fixValue;
    private final boolean isInteger;
    private final long startTime;
    private final long endTime;
    private final String metricName;
    private final Map<String, String> tags;

    public EmptySpan(long interval, double fixValue,
            boolean isInteger, long startTime, long endTime, String metricName) {
        this.interval = interval;
        this.fixValue = fixValue;
        this.isInteger = isInteger;
        this.startTime = startTime;
        this.endTime = endTime;
        this.metricName = metricName;
        this.tags = new HashMap();
    }

    public String metricName() {
        return metricName;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public void addTag(String key, String value) {
        tags.put(key, value);
    }

    public List<String> getAggregatedTags() {
        return Collections.emptyList();
    }

    public int size() {
        return (int) (endTime - startTime) / (int) interval;
    }

    public long timestamp(final int i) {
        return startTime - (startTime % interval) + i * interval;
    }

    public boolean isInteger(final int i) {
        return isInteger;
    }

    public long longValue(final int i) {
        return (long) fixValue;
    }

    public double doubleValue(final int i) {
        return fixValue;
    }

    SeekableView spanIterator() {
        return new EmptyIterator(interval, fixValue, isInteger, startTime, endTime);
    }

    SeekableView downsampler(final int interval, final Aggregator downsampler) {
        return new EmptyIterator(interval, fixValue, isInteger, startTime, endTime);
    }

    final static class EmptyIterator implements SeekableView, DataPoint {

        private long lastTimestamp = 0;

        private final long interval;
        private final double fixValue;
        private final boolean isInteger;
        private final long startTime;
        private final long endTime;
        
        protected EmptyIterator(long interval, double fixValue,
                boolean isInteger, long startTime, long endTime) {
            this.interval = interval;
            this.fixValue = fixValue;
            this.isInteger = isInteger;
            this.startTime = startTime;
            this.endTime = endTime;
        }

        // Iterator interface //

        public boolean hasNext() {
            return lastTimestamp + interval <= endTime;
        }

        public DataPoint next() {
            if (!hasNext()) {
                throw new NoSuchElementException("no more elements");
            }

            if (0 == lastTimestamp) {
                lastTimestamp = startTime - (startTime % interval);
            }

            lastTimestamp += interval;

            return this;
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }

        // SeekableView interface //

        public void seek(final long timestamp) {
            lastTimestamp = timestamp;
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

