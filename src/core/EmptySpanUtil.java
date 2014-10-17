package net.opentsdb.core;

import java.util.HashMap;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.Collections;

import static org.hbase.async.Bytes.ByteMap;
import org.hbase.async.Bytes;

public class EmptySpanUtil {

  private final static byte[] ZERO_INT = {0, 0, 0, 0};

  /**
   * Adds empty spans for missing aggregate series.
   * It checks if all posible timeseries exist, if
   * not creates empty span for missing serie.
   * Empty span returns zeros for each availability datapoint.
   * Function used for availability metrics.
   */
  public static int insertEmptySpans(TreeMap<byte[], Span> spans, TSDB tsdb, long interval,
          long start_time, long end_time, byte[] metric, String metricName, ArrayList<byte[]> tags,
          ArrayList<byte[]> group_bys, ByteMap<byte[][]> group_by_values) {

      int nrows = 0;
      final short metric_width = tsdb.getMetrics().width();
      final short name_width = tsdb.getTagNames().width();
      final short value_width = tsdb.getTagValues().width();
      byte[] key_buf;
      byte[] tag_buf = new byte[name_width];

      ByteMap<ArrayList<byte[]>> tags_map = new ByteMap<ArrayList<byte[]>>();

      for (byte[] tag : tags) {
          byte[] tagk = new byte[name_width];
          byte[] tagv = new byte[value_width];
          System.arraycopy(tag, 0, tagk, 0, name_width);
          System.arraycopy(tag, name_width, tagv, 0, value_width);
          ArrayList<byte[]> tagsv = tags_map.get(tagk);
          if (null == tagsv) {
              tagsv = new ArrayList<byte[]>();
              tags_map.put(tagk, tagsv);
          }
          tagsv.add(tagv);
      }

      if (group_bys != null && group_by_values != null) {
          for (byte[] tagk : group_bys) {
              ArrayList<byte[]> tagsv = tags_map.get(tagk);
              if (null == tagsv) {
                  tagsv = new ArrayList<byte[]>();
                  tags_map.put(tagk, tagsv);
              }
              for (byte[] tagv : group_by_values.get(tagk)) {
                  tagsv.add(tagv);
              }
          }
      } 

      ArrayList<byte[]> tag_keys = new ArrayList<byte[]>(tags_map.keySet());
      int[] index = new int[tag_keys.size()];

      ArrayList<byte[]> row_tags = new ArrayList<byte[]>();

      int i = 0;
      int j = index.length - 1;
      while (tag_keys.size() > 0) {
          byte[] key = tag_keys.get(i);
          ArrayList<byte[]> values = tags_map.get(key);

          if (i == j) {
              if (index[i] == values.size() - 1) {
                  index[i] = 0;
                  if (j > 0)
                      j--;
              } else {
                  index[i] = index[i] + 1;
                  if (j < index.length - 1)
                      j++;
              }
          }

          byte[] tag_kv = new byte[name_width + value_width];
          System.arraycopy(key, 0, tag_kv, 0, name_width);
          System.arraycopy(values.get(index[i]), 0, tag_kv, name_width, value_width);
          row_tags.add(tag_kv);

          if (i == index.length - 1) {
              key_buf = new byte[metric_width + 4 + (name_width + value_width) * row_tags.size()];
              int n = 0;
              System.arraycopy(metric, 0, key_buf, n, metric_width);
              n += metric_width;
              // set timestamp as zero
              System.arraycopy(ZERO_INT, 0, key_buf, n, 4);
              n += 4;
              Collections.sort(row_tags, Bytes.MEMCMP);
              for (byte[] kv : row_tags) {
                  System.arraycopy(kv, 0, key_buf, n, name_width + value_width);
                  n += name_width + value_width;
              }

              if (null == spans.get(key_buf)) {
                  EmptySpan span = new EmptySpan(interval, 0.0, false,
                          start_time, end_time, metricName);

                  for (byte[] kv : row_tags) {
                      byte[] k = new byte[name_width];
                      byte[] v = new byte[value_width];
                      System.arraycopy(kv, 0, k, 0, name_width);
                      System.arraycopy(kv, name_width, v, 0, value_width);
                      String key_str = tsdb.getTagNames().getName(k);
                      String value_str = tsdb.getTagValues().getName(v);
                      span.addTag(key_str, value_str);
                  }

                  spans.put(key_buf, span); 
                  nrows++;
              }

              row_tags = new ArrayList<byte[]>();
          }

          boolean index_done = true;
          for (int a = 0; a < index.length; a++) {
              if (index[a] < tags_map.get(tag_keys.get(a)).size() - 1) {
                  index_done = false;
                  break;
              }
          }
          if (i == index.length -1 && index_done)
              break;

          i = (i + 1) % index.length;
      }

      return nrows;
  }

}
