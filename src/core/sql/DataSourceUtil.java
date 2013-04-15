package net.opentsdb.core.sql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import java.beans.PropertyVetoException;

public final class DataSourceUtil {
    
    private static final Logger LOG = LoggerFactory.getLogger(DataSourceUtil.class);
        
    public static DataSource createPooledDataSource(
            String host, String user, String pass, String db) {
        
        ComboPooledDataSource cpds = new ComboPooledDataSource();
        try {
            cpds.setDriverClass("com.mysql.jdbc.Driver");
        } catch (PropertyVetoException e) {
            LOG.error("Unable to set dirver class: " + e.getMessage());
            return null;
        }
        
        cpds.setAutoCommitOnClose(true);
        
        cpds.setJdbcUrl("jdbc:mysql://" + host + "/" + db);
        cpds.setUser(user);
        cpds.setPassword(pass);
        
        // pool settings
        cpds.setMinPoolSize(10);      
        cpds.setAcquireIncrement(10);
        cpds.setMaxPoolSize(300);
        // test all idle, pooled but unchecked-out connections, every this number of seconds
        cpds.setIdleConnectionTestPeriod(120);
        // asynchronously verify connection at checkin
        cpds.setTestConnectionOnCheckin(true);
        
        return cpds;
    }
    
    public static void close(ResultSet rs, PreparedStatement st, Connection conn) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                LOG.error("Unable to close result set: " + e.getMessage());
            }
        }
        if (st != null) {
            try {
                st.close();
            } catch (SQLException e) {
                LOG.error("Unable to close statement: " + e.getMessage());
            }
        }
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                LOG.error("Unable to close connection: " + e.getMessage());
            }
        }
    }
    
    public static long toLong(byte[] bytes) {
        return ByteBuffer.allocate(8).put(bytes).getLong(0);
    }
    
    public static byte[] toBytes(long l) {
        return ByteBuffer.allocate(8).putLong(l).array();
    }
    
}
