package net.opentsdb.tsd;

import java.util.ArrayList;
import java.io.IOException;

import org.hbase.async.Bytes;
import org.hbase.async.DeleteRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;

import net.opentsdb.core.TSDB;
import net.opentsdb.core.Internal;
import net.opentsdb.core.Query;
import net.opentsdb.core.TSDB;

public class DeleteHandler implements HttpRpc {
    
    public void execute(TSDB tsdb, HttpQuery hquery) throws IOException {
        long deleted_rows = 0;
        try {
            final long start_time = GraphHandler.getQueryStringDate(hquery, "start");
            if (start_time == -1) {
                throw BadRequestException.missingParameter("start");
            }
            long end_time = GraphHandler.getQueryStringDate(hquery, "end");
            if (end_time == -1) {
                long now = System.currentTimeMillis() / 1000;
                end_time = now;
            }

            Query[] queries = GraphHandler.parseQuery(tsdb, hquery);
            HBaseClient client = tsdb.getClient();
            for (final Query query : queries) {
                if (null == query)
                    continue;
                try {
                    query.setStartTime(start_time);
                } catch (IllegalArgumentException e) {
                    throw new BadRequestException("start time: " + e.getMessage());
                }
                try {
                    query.setEndTime(end_time);
                } catch (IllegalArgumentException e) {
                    throw new BadRequestException("end time: " + e.getMessage());
                }
                final Scanner scanner = Internal.getScanner(query);
                ArrayList<ArrayList<KeyValue>> rows;
                while ((rows = scanner.nextRows().joinUninterruptibly()) != null) {
                    for (final ArrayList<KeyValue> row : rows) {
                        byte[] key = row.get(0).key();
                        DeleteRequest del = new DeleteRequest(tsdb.getTable(), key);
                        client.delete(del);
                        deleted_rows++;
                    }
                }
            }

            hquery.sendReply("{\"status\": \"SUCCESS\", \"deleted_rows\": \"" + deleted_rows + "\"}"); 
        } catch (IllegalArgumentException e) {
            hquery.badRequest(e.getMessage());
        } catch (Exception e) {
            hquery.internalError(e);
        }
    }

}

