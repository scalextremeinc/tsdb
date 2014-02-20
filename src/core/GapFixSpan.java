package net.opentsdb.core;

public class GapFixSpan extends Span {

    private final long interval;
    private final double fixValue;
    private final boolean isInteger;
    private final long startTime;
    private final long endTime;
    
    public GapFixSpan(final TSDB tsdb, long interval, double fixValue,
            boolean isInteger, long startTime, long endTime) {
        super(tsdb);
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
                interval, fixValue, isInteger, startTime, endTime);
    }

}
