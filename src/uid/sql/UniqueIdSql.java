package net.opentsdb.uid.sql;

import java.util.concurrent.ConcurrentHashMap;
import java.util.Arrays;

import java.nio.charset.Charset;
import java.nio.ByteBuffer;

import java.sql.Connection;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.uid.NoSuchUniqueId;
import net.opentsdb.uid.UniqueIdInterface;
import net.opentsdb.core.sql.DataSourceUtil;


public final class UniqueIdSql implements UniqueIdInterface {
    
    private static final Logger LOG = LoggerFactory.getLogger(UniqueIdSql.class);
    
    /** Charset used to convert Strings to byte arrays and back. */
    private static final Charset CHARSET = Charset.forName("ISO-8859-1");
    
    private final String select_id_query;
    private final String select_name_query;
    private final String insert_id_query;
    
    private DataSource ds;
    private String table;
    
    /**
     * Cache for forward mappings (name to ID).
     */
    private final ConcurrentHashMap<String, byte[]> nameCache =
        new ConcurrentHashMap<String, byte[]>();
    /** 
     * Cache for backward mappings (ID to name).
     * The ID in the key is a byte[] converted to a String to be Comparable.
     */
    private final ConcurrentHashMap<String, String> idCache =
        new ConcurrentHashMap<String, String>();
    
    /** Number of times we avoided reading from HBase thanks to the cache. */
    private volatile int cacheHits;
    
    /** Number of times we had to read from HBase and populate the cache. */
    private volatile int cacheMisses;
    
    private final short idWidth;
    
    public UniqueIdSql(DataSource ds, String table) {
        this.ds = ds;
        this.table = table;
        idWidth = 8;
        select_id_query = "SELECT id FROM " + table + " WHERE name=?";
        select_name_query = "SELECT name FROM " + table + " WHERE id=?";
        insert_id_query = "INSERT INTO " + table + " (name) VALUES(?)";
    }
    
    public String kind() {
        return table;
    }

    public short width() {
        return idWidth;
    }

    public String getName(byte[] id) throws NoSuchUniqueId {
        if (id.length != idWidth) {
            throw new IllegalArgumentException("Wrong id.length = " + id.length
                                             + " which is != " + idWidth
                                             + " required for '" + kind() + '\'');
        }
        String name = getNameFromCache(id);
        if (name != null) {
            cacheHits++;
        } else {
            cacheMisses++;
            name = getNameFromDb(id);
            if (name == null) {
                throw new NoSuchUniqueId(kind(), id);
            }
            addNameToCache(id, name);
            addIdToCache(name, id);
        }
        return name;
    }

    public byte[] getId(String name) throws NoSuchUniqueName {
        byte[] id = getIdFromCache(name);
        if (id != null) {
            cacheHits++;
        } else {
            cacheMisses++;
            id = getIdFromDb(name);
            if (id == null) {
                throw new NoSuchUniqueName(kind(), name);
            }
            if (id.length != idWidth) {
                throw new IllegalStateException("Found id.length = " + id.length
                                            + " which is != " + idWidth
                                            + " required for '" + kind() + '\'');
            }
            addIdToCache(name, id);
            addNameToCache(id, name);
        }
        return id;
    }

    public byte[] getOrCreateId(String name) {
        byte[] id = null;
        try {
            id = getId(name);
        } catch (NoSuchUniqueName e) {
            // skip
        }
        if (id == null) {
            Connection conn = null;
            PreparedStatement st = null;
            ResultSet rs = null;
            try {
                conn = ds.getConnection();
                st = conn.prepareStatement(insert_id_query, Statement.RETURN_GENERATED_KEYS);
                st.setString(1, name);
                st.executeUpdate();
                
                rs = st.getGeneratedKeys();
                rs.next();
                id = ByteBuffer.allocate(8).putLong(rs.getLong(1)).array();
                
                addIdToCache(name, id);
                addNameToCache(id, name);                
            } catch (SQLException e) {
                LOG.error("Unable to insert name: " + e.getMessage());
            } finally {
                DataSourceUtil.close(rs, st, conn); 
            }
        }
        return id;
    }
    
    private byte[] getIdFromCache(final String name) {
        return nameCache.get(name);
    }
    
    private String getNameFromCache(final byte[] id) {
        return idCache.get(fromBytes(id));
    }
    
    private void addIdToCache(final String name, final byte[] id) {
        byte[] found = nameCache.get(name);
        if (found == null) {
            found = nameCache.putIfAbsent(name,
                                        // Must make a defensive copy to be immune
                                        // to any changes the caller may do on the
                                        // array later on.
                                        Arrays.copyOf(id, id.length));
        }
        if (found != null && !Arrays.equals(found, id)) {
            throw new IllegalStateException("name=" + name + " => id="
                + Arrays.toString(id) + ", already mapped to "
                + Arrays.toString(found));
        }
    }

    private void addNameToCache(final byte[] id, final String name) {
        final String key = fromBytes(id);
        String found = idCache.get(key);
        if (found == null) {
            found = idCache.putIfAbsent(key, name);
        }
        if (found != null && !found.equals(name)) {
            throw new IllegalStateException("id=" + Arrays.toString(id) + " => name="
                + name + ", already mapped to " + found);
        }
    }
    
    private String getNameFromDb(final byte[] id) {
        long idl = ByteBuffer.allocate(8).put(id).getLong();
        Connection conn = null;
        PreparedStatement st = null;
        ResultSet rs = null;
        try {
            conn = ds.getConnection();
            st = conn.prepareStatement(select_name_query);
            st.setLong(1, idl);
            rs = st.executeQuery();
            if (rs.first()) {
                return rs.getString(0);
            }
        } catch (SQLException e) {
            LOG.error("Unable to get name: " + e.getMessage());
        } finally {
            DataSourceUtil.close(rs, st, conn); 
        }
        return null;
    }
    
    private byte[] getIdFromDb(final String name) {
        Connection conn = null;
        PreparedStatement st = null;
        ResultSet rs = null;
        try {
            conn = ds.getConnection();
            st = conn.prepareStatement(select_id_query);
            st.setString(1, name);
            rs = st.executeQuery();
            if (rs.first()) {
                long id = rs.getLong(0);
                return ByteBuffer.allocate(8).putLong(id).array();
            }
        } catch (SQLException e) {
            LOG.error("Unable to get id: " + e.getMessage());
        } finally {
            DataSourceUtil.close(rs, st, conn); 
        }
        return null;
    }
    
    private static byte[] toBytes(final String s) {
        return s.getBytes(CHARSET);
    }

    private static String fromBytes(final byte[] b) {
        return new String(b, CHARSET);
    }
    
}
