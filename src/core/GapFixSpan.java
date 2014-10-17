package net.opentsdb.core;

import java.util.Map;
import java.util.List;
import java.util.NoSuchElementException;

public class GapFixSpan extends Span {

    private final long interval;
    private final double fixValue;
    private final boolean isInteger;
    private final long startTime;
    private final long endTime;
    
    public GapFixSpan(long interval, double fixValue,
            boolean isInteger, long startTime, long endTime) {
        this.interval = interval;
        this.fixValue = fixValue;
        this.isInteger = isInteger;
        this.startTime = startTime;
        this.endTime = endTime;
    }

    SeekableView spanIterator() {
        return new GapFixDataPoints.GapFixIterator(
                new Span.Iterator(), interval, fixValue, isInteger, startTime, endTime);
    }

    SeekableView downsampler(final int interval, final Aggregator downsampler) {
        return new GapFixDataPoints.GapFixIterator(
                new Span.DownsamplingIterator(interval, downsampler),
                this.interval, fixValue, isInteger, startTime, endTime);
    }

}
