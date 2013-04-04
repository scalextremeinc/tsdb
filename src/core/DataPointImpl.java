package net.opentsdb.core;

import java.lang.ClassCastException;

public class DataPointImpl implements DataPoint {
    
    private long ts;
    private long ival;
    private double dval;
    private boolean isInteger;
    
    public DataPointImpl(long ts, long ival) {
        this.ts = ts;
        this.ival = ival;
        this.isInteger = true;
    }
  
    public DataPointImpl(long ts, double dval) {
        this.ts = ts;
        this.dval = dval;
        this.isInteger = false;
    }

    public long timestamp() {
        return ts;
    }

    public boolean isInteger() {
        return isInteger;
    }

    public long longValue() {
        if (!isInteger())
            throw new ClassCastException("DataPoint contains double value");
        return ival;
    }

    public double doubleValue() {
        if (isInteger())
            throw new ClassCastException("DataPoint contains integer value");
        return dval;
    }

    public double toDouble() {
        if (isInteger())
            return (double) ival;
        else
            return dval;
    }

}
