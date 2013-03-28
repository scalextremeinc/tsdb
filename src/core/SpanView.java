package net.opentsdb.core;

import java.util.Map;

public interface SpanView extends DataPoints {
    
    SpanViewIterator internalIterator();

}
