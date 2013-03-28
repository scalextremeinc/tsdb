package net.opentsdb.core;

import java.util.Map;

public interface SpanViewIterator extends SeekableView, DataPoint {
  
  /** Helper to take a snapshot of the state of this iterator.  */
  int saveState();

  /** Helper to restore a snapshot of the state of this iterator.  */
  void restoreState(int state);

  /**
   * Look a head to see the next timestamp.
   * @throws IndexOutOfBoundsException if we reached the end already.
   */
  long peekNextTimestamp();
  
  /** Only returns internal state for the iterator itself.  */
  String toStringSummary();
  
}
