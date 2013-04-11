// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a read-only sequence of continuous data points.
 * <p>
 * This class stores a continuous sequence of {@link SpanView}s in memory.
 */
final public class Span implements DataPoints {

  private static final Logger LOG = LoggerFactory.getLogger(Span.class);

  /** All the rows in this span. */
  private List<? extends SpanView> rows;

  private void checkNotEmpty() {
    if (rows.size() == 0) {
      throw new IllegalStateException("empty Span");
    }
  }

  public String metricName() {
    checkNotEmpty();
    return rows.get(0).metricName();
  }

  public Map<String, String> getTags() {
    checkNotEmpty();
    return rows.get(0).getTags();
  }

  public List<String> getAggregatedTags() {
    return Collections.emptyList();
  }

  public int size() {
    int size = 0;
    for (final SpanView row : rows) {
      size += row.size();
    }
    return size;
  }

  public int aggregatedSize() {
    return 0;
  }

  public void setSpanViews(List<? extends SpanView> rows) {
      this.rows = rows;
  }

  public SeekableView iterator() {
    return spanIterator();
  }

  /**
   * Finds the index of the row of the ith data point and the offset in the row.
   * @param i The index of the data point to find.
   * @return two ints packed in a long.  The first int is the index of the row
   * in {@code rows} and the second is offset in that {@link SpanView} instance.
   */
  private long getIdxOffsetFor(final int i) {
    int idx = 0;
    int offset = 0;
    for (final SpanView row : rows) {
      final int sz = row.size();
      if (offset + sz > i) {
        break;
      }
      offset += sz;
      idx++;
    }
    return ((long) idx << 32) | (i - offset);
  }

  public long timestamp(final int i) {
    final long idxoffset = getIdxOffsetFor(i);
    final int idx = (int) (idxoffset >>> 32);
    final int offset = (int) (idxoffset & 0x00000000FFFFFFFF);
    return rows.get(idx).timestamp(offset);
  }

  public boolean isInteger(final int i) {
    final long idxoffset = getIdxOffsetFor(i);
    final int idx = (int) (idxoffset >>> 32);
    final int offset = (int) (idxoffset & 0x00000000FFFFFFFF);
    return rows.get(idx).isInteger(offset);
  }

  public long longValue(final int i) {
    final long idxoffset = getIdxOffsetFor(i);
    final int idx = (int) (idxoffset >>> 32);
    final int offset = (int) (idxoffset & 0x00000000FFFFFFFF);
    return rows.get(idx).longValue(offset);
  }

  public double doubleValue(final int i) {
    final long idxoffset = getIdxOffsetFor(i);
    final int idx = (int) (idxoffset >>> 32);
    final int offset = (int) (idxoffset & 0x00000000FFFFFFFF);
    return rows.get(idx).doubleValue(offset);
  }

  /** Returns a human readable string representation of the object. */
  public String toString() {
    final StringBuilder buf = new StringBuilder();
    buf.append("Span(")
       .append(rows.size())
       .append(" rows, [");
    for (int i = 0; i < rows.size(); i++) {
      if (i != 0) {
        buf.append(", ");
      }
      buf.append(rows.get(i).toString());
      buf.append(", ");
      buf.append(rows.get(i).size());
    }
    buf.append("])");
    return buf.toString();
  }

  /**
   * Finds the index of the row in which the given timestamp should be.
   * @param timestamp A strictly positive 32-bit integer.
   * @return A strictly positive index in the {@code rows} array.
   */
  private short seekRow(final long timestamp) {
    short row_index = 0;
    SpanView row = null;
    final int nrows = rows.size();
    for (int i = 0; i < nrows; i++) {
      row = rows.get(i);
      final int sz = row.size();
      if (row.timestamp(sz - 1) < timestamp) {
        row_index++;  // The last DP in this row is before 'timestamp'.
      } else {
        break;
      }
    }
    if (row_index == nrows) {  // If this timestamp was too large for the
      --row_index;             // last row, return the last row.
    }
    return row_index;
  }

  /** Package private iterator method to access it as a Span.Iterator. */
  Span.Iterator spanIterator() {
    return new Span.Iterator();
  }

  /** Iterator for {@link Span}s. */
  final class Iterator implements SeekableView {

    /** Index of the {@link SpanView} we're currently at, in {@code rows}. */
    private short row_index;

    /** Iterator on the current row. */
    private SeekableView current_row;

    Iterator() {
      current_row = rows.get(0).internalIterator();
    }

    public boolean hasNext() {
      return (current_row.hasNext()             // more points in this row
              || row_index < rows.size() - 1);  // or more rows
    }

    public DataPoint next() {
      if (current_row.hasNext()) {
        return current_row.next();
      } else if (row_index < rows.size() - 1) {
        row_index++;
        current_row = rows.get(row_index).internalIterator();
        return current_row.next();
      }
      throw new NoSuchElementException("no more elements");
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }

    public void seek(final long timestamp) {
      short row_index = seekRow(timestamp);
      if (row_index != this.row_index) {
        this.row_index = row_index;
        current_row = rows.get(row_index).internalIterator();
      }
      current_row.seek(timestamp);
    }

    public String toString() {
      return "Span.Iterator(row_index=" + row_index
        + ", current_row=" + current_row + ", span=" + Span.this + ')';
    }

  }

  /** Package private iterator method to access it as a DownsamplingIterator. */
  Span.DownsamplingIterator downsampler(final int interval,
                                        final Aggregator downsampler) {
    return new Span.DownsamplingIterator(interval, downsampler);
  }

  /**
   * Iterator that downsamples the data using an {@link Aggregator}.
   * <p>
   * This implementation relies on the fact that the {@link SpanView}s in this
   * {@link Span} have {@code O(1)} access to individual data points, in order
   * to be efficient.
   */
  final class DownsamplingIterator
    implements SeekableView, DataPoint,
               Aggregator.Longs, Aggregator.Doubles {

    /** Extra bit we set on the timestamp of floating point values. */
    private static final long FLAG_FLOAT = 0x8000000000000000L;

    /** Mask to use in order to get rid of the flag above. */
    private static final long TIME_MASK  = 0x7FFFFFFFFFFFFFFFL;

    /** The "sampling" interval, in seconds. */
    private final int interval;

    /** Function to use to for downsampling. */
    private final Aggregator downsampler;

    /** Index of the {@link SpanView} we're currently at, in {@code rows}. */
    private short row_index;

    /** The row we're currently at. */
    private SpanViewIterator current_row;

    /**
     * Current timestamp (unsigned 32 bits).
     * The most significant bit is used to store FLAG_FLOAT.
     */
    private long time;

    /** Current value (either an actual long or a double encoded in a long). */
    private long value;

    /**
     * Ctor.
     * @param interval The interval in seconds wanted between each data point.
     * @param downsampler The downsampling function to use.
     * @param iterator The iterator to access the underlying data.
     */
    DownsamplingIterator(final int interval,
                         final Aggregator downsampler) {
      this.interval = interval;
      this.downsampler = downsampler;
      this.current_row = rows.get(0).internalIterator();
    }

    // ------------------ //
    // Iterator interface //
    // ------------------ //

    public boolean hasNext() {
      return (current_row.hasNext()             // more points in this row
              || row_index < rows.size() - 1);  // or more rows
    }

    private boolean moveToNext() {
      if (!current_row.hasNext()) {
        // Yes, move on to the next one.
        if (row_index < rows.size() - 1) {  // Do we have more rows?
          current_row = rows.get(++row_index).internalIterator();
          current_row.next();  // Position the iterator on the first element.
          return true;
        } else {  // No more rows, can't go further.
          return false;
        }
      }
      current_row.next();
      return true;
    }

    public DataPoint next() {
      if (!hasNext()) {
        throw new NoSuchElementException("no more data points in " + this);
      }

      // Look ahead to see if all the data points that fall within the next
      // interval turn out to be integers.  While we do this, compute the
      // average timestamp of all the datapoints in that interval.
      long newtime = 0;
      final short saved_row_index = row_index;
      final int saved_state = current_row.saveState();
      // Since we know hasNext() returned true, we have at least 1 point.
      moveToNext();
      time = current_row.timestamp() + interval;  // end of this interval.
      boolean integer = true;
      int npoints = 0;
      do {
        npoints++;
        newtime += current_row.timestamp();
        //LOG.debug("Downsampling @ time " + current_row.timestamp());
        integer &= current_row.isInteger();
      } while (moveToNext() && current_row.timestamp() < time);
      newtime /= npoints;

      // Now that we're done looking ahead, let's go back where we were.
      if (row_index != saved_row_index) {
        row_index = saved_row_index;
        current_row = rows.get(row_index).internalIterator();
      }
      current_row.restoreState(saved_state);

      // Compute `value'.  This will rely on `time' containing the end time of
      // this interval...
      if (integer) {
        value = downsampler.runLong(this);
      } else {
        value = Double.doubleToRawLongBits(downsampler.runDouble(this));
      }
      // ... so update the time only here.
      time = newtime;
      //LOG.info("Downsampled avg time " + time);
      if (!integer) {
        time |= FLAG_FLOAT;
      }
      return this;
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }

    // ---------------------- //
    // SeekableView interface //
    // ---------------------- //

    public void seek(final long timestamp) {
      short row_index = seekRow(timestamp);
      if (row_index != this.row_index) {
        //LOG.debug("seek from row #" + this.row_index + " to " + row_index);
        this.row_index = row_index;
        current_row = rows.get(row_index).internalIterator();
      }
      current_row.seek(timestamp);
    }

    // ------------------- //
    // DataPoint interface //
    // ------------------- //

    public long timestamp() {
      return time & TIME_MASK;
    }

    public boolean isInteger() {
      return (time & FLAG_FLOAT) == 0;
    }

    public long longValue() {
      if (isInteger()) {
        return value;
      }
      throw new ClassCastException("this value is not a long in " + this);
    }

    public double doubleValue() {
      if (!isInteger()) {
        return Double.longBitsToDouble(value);
      }
      throw new ClassCastException("this value is not a float in " + this);
    }

    public double toDouble() {
      return isInteger() ? longValue() : doubleValue();
    }

    // -------------------------- //
    // Aggregator.Longs interface //
    // -------------------------- //

    public boolean hasNextValue() {
      if (!current_row.hasNext()) {
        if (row_index < rows.size() - 1) {
          //LOG.info("hasNextValue: next row? " + (rows.get(row_index + 1).timestamp(0) < time));
          return rows.get(row_index + 1).timestamp(0) < time;
        } else {
          //LOG.info("hasNextValue: false, this is the end");
          return false;
        }
      }
      //LOG.info("hasNextValue: next point? " + (current_row.peekNextTimestamp() < time));
      return current_row.peekNextTimestamp() < time;
    }

    public long nextLongValue() {
      if (hasNextValue()) {
        moveToNext();
        return current_row.longValue();
      }
      throw new NoSuchElementException("no more longs in interval of " + this);
    }

    // ---------------------------- //
    // Aggregator.Doubles interface //
    // ---------------------------- //

    public double nextDoubleValue() {
      if (hasNextValue()) {
        moveToNext();
        // Use `toDouble' instead of `doubleValue' because we can get here if
        // there's a mix of integer values and floating point values in the
        // current downsampled interval.
        return current_row.toDouble();
      }
      throw new NoSuchElementException("no more floats in interval of " + this);
    }

    public String toString() {
      final StringBuilder buf = new StringBuilder();
      buf.append("Span.DownsamplingIterator(interval=").append(interval)
         .append(", downsampler=").append(downsampler)
         .append(", row_index=").append(row_index)
         .append(", current_row=").append(current_row.toStringSummary())
         .append("), current time=").append(timestamp())
         .append(", current value=");
     if (isInteger()) {
       buf.append("long:").append(longValue());
     } else {
       buf.append("double:").append(doubleValue());
     }
     buf.append(", rows=").append(rows).append(')');
     return buf.toString();
    }

  }

}
