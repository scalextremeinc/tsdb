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

import java.util.Arrays;
import java.util.List;
import java.util.Collections;
import java.util.Iterator;

import org.hbase.async.Bytes;

/** Helper functions to deal with the row key. */
public final class RowKey {

    private final static byte[] ZERO_INT = {0, 0, 0, 0};

    private RowKey() {
        // Can't create instances of this utility class.
    }

    /**
     * Extracts the name of the metric ID contained in a row key.
     * @param tsdb The TSDB to use.
     * @param row The actual row key.
     * @return The name of the metric.
     */
    public static String metricName(final TSDB tsdb, final byte[] row) {
        final byte[] id = Arrays.copyOfRange(row, 0, tsdb.getMetrics().width());
        return tsdb.getMetrics().getName(id);
    }

    public static byte[] getTagKey(final TSDB tsdb, byte[] tag_kv) {
        short name_width = tsdb.getTagNames().width();
        byte[] tag_k = new byte[name_width];
        System.arraycopy(tag_kv, 0, tag_k, 0, name_width);
        return tag_k;
    }

    public static byte[] getTagVal(final TSDB tsdb, byte[] tag_kv) {
        short name_width = tsdb.getTagNames().width();
        short value_width = tsdb.getTagValues().width();
        byte[] tag_v = new byte[value_width];
        System.arraycopy(tag_kv, name_width, tag_v, 0, value_width);
        return tag_v;
    }

    public static byte[] createRowKey(final TSDB tsdb, byte[] metric, List<byte[]> tag_kvs) {
        short metric_width = tsdb.getMetrics().width();
        short name_width = tsdb.getTagNames().width();
        short value_width = tsdb.getTagValues().width();

        byte[] key_buf = new byte[metric_width + ZERO_INT.length + (name_width + value_width) * tag_kvs.size()];
        int n = 0;
        System.arraycopy(metric, 0, key_buf, n, metric_width);
        n += metric_width;
        // set timestamp as zero
        System.arraycopy(ZERO_INT, 0, key_buf, n, ZERO_INT.length);
        n += ZERO_INT.length;
        Collections.sort(tag_kvs, Bytes.MEMCMP);
        for (byte[] kv : tag_kvs) {
            System.arraycopy(kv, 0, key_buf, n, name_width + value_width);
            n += name_width + value_width;
        }

        return key_buf;
    }

    public static class TagIterator implements Iterator<byte[]> {

        private TSDB tsdb;
        private byte[] key;

        private int index;
        private int type;
        private short[] nv_width = new short[2];

        public TagIterator(TSDB tsdb, byte[] key) {
            this.tsdb = tsdb;
            this.key = key;
            index = tsdb.getMetrics().width() + ZERO_INT.length;
            type = 0;
            nv_width[0] = tsdb.getTagNames().width();
            nv_width[1] = tsdb.getTagValues().width();
        }

        public boolean hasNext() {
            return index < key.length;
        }

        public byte[] next() {
            short width = nv_width[type];
            byte[] buf = new byte[width];
            
            System.arraycopy(key, index, buf, 0, width);

            index += width;
            type = (type + 1) % 2;

            return buf;
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }

    }

}
