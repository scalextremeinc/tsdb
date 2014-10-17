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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
final public class TsdbQuery implements Query {

  private static final Logger LOG = LoggerFactory.getLogger(TsdbQuery.class);

  /**
   * Charset to use with our server-side row-filter.
   * We use this one because it preserves every possible byte unchanged.
   */
  static final Charset CHARSET = Charset.forName("ISO-8859-1");

  private static final Pattern availPattern = Pattern.compile("avail[^0-9]*([0-9]+)");

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
  private String metricName;

  /**
   * Tags of the metrics being looked up.
   * Each tag is a byte array holding the ID of both the name and value
   * of the tag.
   * Invariant: an element cannot be both in this array and in group_bys.
   */
  private ArrayList<byte[]> tags;
  
  /**
   * Tags specified in query as tag=<empty>, this allows to query for data without particular tag.
   */
  private ArrayList<byte[]> empty_tags = new ArrayList<byte[]>();

  /**
   * Tags by which we must group the results.
   * Each element is a tag ID.
   * Invariant: an element cannot be both in this array and in {@code tags}.
   */
  private ArrayList<byte[]> group_bys;
  
  /**
   * "Tags" created when doing aggregation with '+',
   * they don't exist in storage - needed for serie names.
   */
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
  
  private Map<byte[], Boolean> plus_aggregate = new HashMap<byte[], Boolean>();
  
  private StorageQuery storage_query;

  private Boolean isAvail;
  private Long availInterval;

  /** Constructor. */
  public TsdbQuery(final TSDB tsdb) {
    this.tsdb = tsdb;
  }
  
  public void setStorageQuery(StorageQuery storage_query) {
      this.storage_query = storage_query;
  }

  public void setStartTime(long timestamp) {
    if ((timestamp & 0xFFFFFFFF00000000L) != 0) {
      throw new IllegalArgumentException("Invalid timestamp: " + timestamp);
    } else if (end_time != UNSET && timestamp >= getEndTime()) {
      throw new IllegalArgumentException("new start time (" + timestamp
          + ") is greater than or equal to end time: " + getEndTime());
    }
    // align availability to its interval
    if (isAvailability())
        timestamp = timestamp  - (timestamp % getAvailInterval()) - getAvailInterval();
    // Keep the 32 bits.
    start_time = (int) timestamp;
  }

  public long getStartTime() {
    if (start_time == UNSET) {
      throw new IllegalStateException("setStartTime was never called!");
    }
    return start_time & 0x00000000FFFFFFFFL;
  }

  public void setEndTime(long timestamp) {
    if ((timestamp & 0xFFFFFFFF00000000L) != 0) {
      throw new IllegalArgumentException("Invalid timestamp: " + timestamp);
    } else if (start_time != UNSET && timestamp <= getStartTime()) {
      throw new IllegalArgumentException("new end time (" + timestamp
          + ") is less than or equal to start time: " + getStartTime());
    }
    // align availability to its interval
    if (isAvailability())
        timestamp = timestamp  - (timestamp % getAvailInterval()) + getAvailInterval();
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
    this.metricName = metric;
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
      if ("<empty>".equals(tagvalue)) {
        LOG.info("Empty tag: " + tag.getKey());
        byte[] tag_id = tsdb.getTagNames().getId(tag.getKey());
        empty_tags.add(tag_id);
        continue;
      }
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
        	plus_aggregate.put(tag_id, Boolean.FALSE);
        } else {
            if (extra_tags == null)
                extra_tags = new HashMap<String, String>();
            extra_tags.put(tag.getKey(), tag.getValue().replace(' ', '+'));
        	values = Tags.splitString(tagvalue, ' ');
        	plus_aggregate.put(tag_id, Boolean.TRUE);
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
    storage_query.setMetricName(metricName);
    storage_query.setScanStartTime(start_time);
    storage_query.setScanEndTime(end_time);
    storage_query.setTags(tags);
    storage_query.setEmptyTags(empty_tags);
    storage_query.setGroupBys(group_bys);
    storage_query.setGroupByValues(group_by_values);
    storage_query.setRate(rate);
    storage_query.serAggregator(aggregator);
    storage_query.setDownsampler(downsampler);
    storage_query.setSampleInterval(sample_interval);
    storage_query.setPlusAggregate(plus_aggregate);
    storage_query.setExtraTags(extra_tags);
    storage_query.setIsAvail(isAvailability());
    storage_query.setAvailInterval(getAvailInterval());
    return storage_query.runQuery();
  }

  private boolean isAvailability() {
      if (null == isAvail)
          isAvail = metricName.contains("system.uptime.availability")
              || (metricName.contains(".avail")
                      && (metricName.contains(".percent") || metricName.contains(".second")));

      return isAvail;
  }

  private long getAvailInterval() {
      if (null == availInterval) {
          Matcher matcher = availPattern.matcher(metricName);
          if (matcher.find())
              availInterval = Long.parseLong(matcher.group(1));
          else
              availInterval = 3600L; 
      }

      return availInterval;
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
