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

import java.nio.charset.Charset;
import java.util.List;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.lang.Boolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.hbase.async.Bytes;
import static org.hbase.async.Bytes.ByteMap;

import net.opentsdb.stats.Histogram;
import net.opentsdb.uid.NoSuchUniqueId;
import net.opentsdb.uid.NoSuchUniqueName;

/**
 * Non-synchronized implementation of {@link Query}.
 */
final class TsdbQuery implements Query {

  private static final Logger LOG = LoggerFactory.getLogger(TsdbQuery.class);

  /** Used whenever there are no results. */
  private static final DataPoints[] NO_RESULT = new DataPoints[0];

  /**
   * Charset to use with our server-side row-filter.
   * We use this one because it preserves every possible byte unchanged.
   */
  static final Charset CHARSET = Charset.forName("ISO-8859-1");

  /** The TSDB we belong to. */
  private final TSDB tsdb;

  /** Value used for timestamps that are uninitialized.  */
  static final int UNSET = -1;

  /** Start time (UNIX timestamp in seconds) on 32 bits ("unsigned" int). */
  private int start_time = UNSET;

  /** End time (UNIX timestamp in seconds) on 32 bits ("unsigned" int). */
  private int end_time = UNSET;

  /** ID of the metric being looked up. */
  private byte[] metric;

  /**
   * Tags of the metrics being looked up.
   * Each tag is a byte array holding the ID of both the name and value
   * of the tag.
   * Invariant: an element cannot be both in this array and in group_bys.
   */
  private ArrayList<byte[]> tags;

  /**
   * Tags by which we must group the results.
   * Each element is a tag ID.
   * Invariant: an element cannot be both in this array and in {@code tags}.
   */
  private ArrayList<byte[]> group_bys;
  
  private Map<String, String> extra_tags;

  /**
   * Values we may be grouping on.
   * For certain elements in {@code group_bys}, we may have a specific list of
   * values IDs we're looking for.  Those IDs are stored in this map.  The key
   * is an element of {@code group_bys} (so a tag name ID) and the values are
   * tag value IDs (at least two).
   */
  private ByteMap<byte[][]> group_by_values;

  /** If true, use rate of change instead of actual values. */
  private boolean rate;

  /** Aggregator function to use. */
  private Aggregator aggregator;

  /**
   * Downsampling function to use, if any (can be {@code null}).
   * If this is non-null, {@code sample_interval} must be strictly positive.
   */
  private Aggregator downsampler;

  /** Minimum time interval (in seconds) wanted between each data point. */
  private int sample_interval;
  
  private Map<byte[], Boolean> aggregate_tag = new HashMap<byte[], Boolean>();
  
  private StorageQuery storage_query;

  /** Constructor. */
  public TsdbQuery(final TSDB tsdb) {
    this.tsdb = tsdb;
  }
  
  public void setStorageQuery(StorageQuery storage_query) {
      this.storage_query = storage_query;
  }

  public void setStartTime(final long timestamp) {
    if ((timestamp & 0xFFFFFFFF00000000L) != 0) {
      throw new IllegalArgumentException("Invalid timestamp: " + timestamp);
    } else if (end_time != UNSET && timestamp >= getEndTime()) {
      throw new IllegalArgumentException("new start time (" + timestamp
          + ") is greater than or equal to end time: " + getEndTime());
    }
    // Keep the 32 bits.
    start_time = (int) timestamp;
  }

  public long getStartTime() {
    if (start_time == UNSET) {
      throw new IllegalStateException("setStartTime was never called!");
    }
    return start_time & 0x00000000FFFFFFFFL;
  }

  public void setEndTime(final long timestamp) {
    if ((timestamp & 0xFFFFFFFF00000000L) != 0) {
      throw new IllegalArgumentException("Invalid timestamp: " + timestamp);
    } else if (start_time != UNSET && timestamp <= getStartTime()) {
      throw new IllegalArgumentException("new end time (" + timestamp
          + ") is less than or equal to start time: " + getStartTime());
    }
    // Keep the 32 bits.
    end_time = (int) timestamp;
  }

  public long getEndTime() {
    if (end_time == UNSET) {
      setEndTime(System.currentTimeMillis() / 1000);
    }
    return end_time;
  }

  public void setTimeSeries(final String metric,
                            final Map<String, String> tags,
                            final Aggregator function,
                            final boolean rate) throws NoSuchUniqueName {
    findGroupBys(tags);
    this.metric = tsdb.getMetrics().getId(metric);
    this.tags = Tags.resolveAll(tsdb, tags);
    aggregator = function;
    this.rate = rate;
  }

  public void downsample(final int interval, final Aggregator downsampler) {
    if (downsampler == null) {
      throw new NullPointerException("downsampler");
    } else if (interval <= 0) {
      throw new IllegalArgumentException("interval not > 0: " + interval);
    }
    this.downsampler = downsampler;
    this.sample_interval = interval;
  }

  /**
   * Extracts all the tags we must use to group results.
   * <ul>
   * <li>If a tag has the form {@code name=*} then we'll create one
   *     group per value we find for that tag.</li>
   * <li>If a tag has the form {@code name={v1,v2,..,vN}} then we'll
   *     create {@code N} groups.</li>
   * </ul>
   * In the both cases above, {@code name} will be stored in the
   * {@code group_bys} attribute.  In the second case specifically,
   * the {@code N} values would be stored in {@code group_by_values},
   * the key in this map being {@code name}.
   * @param tags The tags from which to extract the 'GROUP BY's.
   * Each tag that represents a 'GROUP BY' will be removed from the map
   * passed in argument.
   */
  private void findGroupBys(final Map<String, String> tags) {
    final Iterator<Map.Entry<String, String>> i = tags.entrySet().iterator();
    while (i.hasNext()) {
      final Map.Entry<String, String> tag = i.next();
      final String tagvalue = tag.getValue();
      if (tagvalue.equals("*")  // 'GROUP BY' with any value.
          || tagvalue.indexOf('|', 1) >= 0 || tagvalue.indexOf(' ', 1) >= 0) {  // Multiple possible values.
        if (group_bys == null) {
          group_bys = new ArrayList<byte[]>();
        }
        byte[] tag_id = tsdb.getTagNames().getId(tag.getKey());
        group_bys.add(tag_id);
        i.remove();
        if (tagvalue.charAt(0) == '*') {
          continue;  // For a 'GROUP BY' with any value, we're done.
        }
        // 'GROUP BY' with specific values.  Need to split the values
        // to group on and store their IDs in group_by_values.
        String[] values = null;
        if (tagvalue.indexOf('|', 1) >= 0) {
        	values = Tags.splitString(tagvalue, '|');
        	aggregate_tag.put(tag_id, Boolean.FALSE);
        } else {
            if (extra_tags == null)
                extra_tags = new HashMap<String, String>();
            extra_tags.put(tag.getKey(), tag.getValue().replace(' ', '+'));
        	values = Tags.splitString(tagvalue, ' ');
        	aggregate_tag.put(tag_id, Boolean.TRUE);
        }
        if (group_by_values == null) {
          group_by_values = new ByteMap<byte[][]>();
        }
        
        LinkedList<byte[]> value_ids_lst = new LinkedList<byte[]>();
        for (int j = 0; j < values.length; j++) {
          try {
            final byte[] value_id = tsdb.getTagValues().getId(values[j]);
            value_ids_lst.add(value_id);
          } catch (NoSuchUniqueName e) {
            LOG.warn("Skipping unknown tag value: " + values[j]);
          } 
        }
        
        final short value_width = tsdb.getTagValues().width();
        final byte[][] value_ids = value_ids_lst.toArray(
            new byte[value_ids_lst.size()][value_width]);
        group_by_values.put(tag_id, value_ids);
      }
    }
  }

  public DataPoints[] run() throws StorageException {
    storage_query.setMetric(metric);
    storage_query.setScanStartTime(start_time);
    storage_query.setScanEndTime(end_time);
    storage_query.setTags(tags);
    storage_query.setGroupBys(group_bys);
    storage_query.setGroupByValues(group_by_values);
    return groupByAndAggregate(storage_query.findSpans());
  }



  /**
   * Creates the {@link SpanGroup}s to form the final results of this query.
   * @param spans The {@link Span}s found for this query ({@link #findSpans}).
   * Can be {@code null}, in which case the array returned will be empty.
   * @return A possibly empty array of {@link SpanGroup}s built according to
   * any 'GROUP BY' formulated in this query.
   */
  private DataPoints[] groupByAndAggregate(final TreeMap<byte[], Span> spans) {
    if (spans == null || spans.size() <= 0) {
      return NO_RESULT;
    }
    if (group_bys == null) {
      // We haven't been asked to find groups, so let's put all the spans
      // together in the same group.
      final SpanGroup group = new SpanGroup(tsdb,
                                            getScanStartTime(),
                                            getScanEndTime(),
                                            spans.values(),
                                            rate,
                                            aggregator,
                                            sample_interval, downsampler);
      return new SpanGroup[] { group };
    }

    // Maps group value IDs to the SpanGroup for those values.  Say we've
    // been asked to group by two things: foo=* bar=* Then the keys in this
    // map will contain all the value IDs combinations we've seen.  If the
    // name IDs for `foo' and `bar' are respectively [0, 0, 7] and [0, 0, 2]
    // then we'll have group_bys=[[0, 0, 2], [0, 0, 7]] (notice it's sorted
    // by ID, so bar is first) and say we find foo=LOL bar=OMG as well as
    // foo=LOL bar=WTF and that the IDs of the tag values are:
    //   LOL=[0, 0, 1]  OMG=[0, 0, 4]  WTF=[0, 0, 3]
    // then the map will have two keys:
    //   - one for the LOL-OMG combination: [0, 0, 1, 0, 0, 4] and,
    //   - one for the LOL-WTF combination: [0, 0, 1, 0, 0, 3].
    final ByteMap<SpanGroup> groups = new ByteMap<SpanGroup>();
    final short value_width = tsdb.getTagValues().width();
    final byte[] group = new byte[group_bys.size() * value_width];
    for (final Map.Entry<byte[], Span> entry : spans.entrySet()) {
      final byte[] row = entry.getKey();
      byte[] value_id = null;
      int i = 0;
      // TODO(tsuna): The following loop has a quadratic behavior.  We can
      // make it much better since both the row key and group_bys are sorted.
      for (final byte[] tag_id : group_bys) {
    	Boolean is_aggregate = aggregate_tag.get(tag_id);
    	if (is_aggregate != null && is_aggregate) {
    		value_id = tag_id;
    	} else {
    		value_id = Tags.getValueId(tsdb, row, tag_id);
    	}
        if (value_id == null) {
          break;
        }
        System.arraycopy(value_id, 0, group, i, value_width);
        i += value_width;
      }
      if (value_id == null) {
        LOG.error("WTF?  Dropping span for row " + Arrays.toString(row)
                 + " as it had no matching tag from the requested groups,"
                 + " which is unexpected.  Query=" + this);
        continue;
      }
      //LOG.info("Span belongs to group " + Arrays.toString(group) + ": " + Arrays.toString(row));
      SpanGroup thegroup = groups.get(group);
      if (thegroup == null) {
        thegroup = new SpanGroup(tsdb, getScanStartTime(), getScanEndTime(),
                                 null, rate, aggregator,
                                 sample_interval, downsampler);
        thegroup.setExtraTags(extra_tags);
        // Copy the array because we're going to keep `group' and overwrite
        // its contents.  So we want the collection to have an immutable copy.
        final byte[] group_copy = new byte[group.length];
        System.arraycopy(group, 0, group_copy, 0, group.length);
        groups.put(group_copy, thegroup);
      }
      thegroup.add(entry.getValue());
    }
    //for (final Map.Entry<byte[], SpanGroup> entry : groups) {
    //  LOG.info("group for " + Arrays.toString(entry.getKey()) + ": " + entry.getValue());
    //}
    return groups.values().toArray(new SpanGroup[groups.size()]);
  }

  /** Returns the UNIX timestamp from which we must start scanning.  */
  private long getScanStartTime() {
    // The reason we look before by `MAX_TIMESPAN * 2' seconds is because of
    // the following.  Let's assume MAX_TIMESPAN = 600 (10 minutes) and the
    // start_time = ... 12:31:00.  If we initialize the scanner to look
    // only 10 minutes before, we'll start scanning at time=12:21, which will
    // give us the row that starts at 12:30 (remember: rows are always aligned
    // on MAX_TIMESPAN boundaries -- so in this example, on 10m boundaries).
    // But we need to start scanning at least 1 row before, so we actually
    // look back by twice MAX_TIMESPAN.  Only when start_time is aligned on a
    // MAX_TIMESPAN boundary then we'll mistakenly scan back by an extra row,
    // but this doesn't really matter.
    // Additionally, in case our sample_interval is large, we need to look
    // even further before/after, so use that too.
    final long ts = getStartTime() - Const.MAX_TIMESPAN * 2 - sample_interval;
    return ts > 0 ? ts : 0;
  }

  /** Returns the UNIX timestamp at which we must stop scanning.  */
  private long getScanEndTime() {
    // For the end_time, we have a different problem.  For instance if our
    // end_time = ... 12:30:00, we'll stop scanning when we get to 12:40, but
    // once again we wanna try to look ahead one more row, so to avoid this
    // problem we always add 1 second to the end_time.  Only when the end_time
    // is of the form HH:59:59 then we will scan ahead an extra row, but once
    // again that doesn't really matter.
    // Additionally, in case our sample_interval is large, we need to look
    // even further before/after, so use that too.
    return getEndTime() + Const.MAX_TIMESPAN + 1 + sample_interval;
  }

  public String toString() {
    final StringBuilder buf = new StringBuilder();
    buf.append("TsdbQuery(start_time=")
       .append(getStartTime())
       .append(", end_time=")
       .append(getEndTime())
       .append(", metric=").append(Arrays.toString(metric));
    try {
      buf.append(" (").append(tsdb.getMetrics().getName(metric));
    } catch (NoSuchUniqueId e) {
      buf.append(" (<").append(e.getMessage()).append('>');
    }
    try {
      buf.append("), tags=").append(Tags.resolveIds(tsdb, tags));
    } catch (NoSuchUniqueId e) {
      buf.append("), tags=<").append(e.getMessage()).append('>');
    }
    buf.append(", rate=").append(rate)
       .append(", aggregator=").append(aggregator)
       .append(", group_bys=(");
    if (group_bys != null) {
      for (final byte[] tag_id : group_bys) {
        try {
          buf.append(tsdb.getTagNames().getName(tag_id));
        } catch (NoSuchUniqueId e) {
          buf.append('<').append(e.getMessage()).append('>');
        }
        buf.append(' ')
           .append(Arrays.toString(tag_id));
        if (group_by_values != null) {
          final byte[][] value_ids = group_by_values.get(tag_id);
          if (value_ids == null) {
            continue;
          }
          buf.append("={");
          for (final byte[] value_id : value_ids) {
            try {
              buf.append(tsdb.getTagValues().getName(value_id));
            } catch (NoSuchUniqueId e) {
              buf.append('<').append(e.getMessage()).append('>');
            }
            buf.append(' ')
               .append(Arrays.toString(value_id))
               .append(", ");
          }
          buf.append('}');
        }
        buf.append(", ");
      }
    }
    buf.append("))");
    return buf.toString();
  }

}
