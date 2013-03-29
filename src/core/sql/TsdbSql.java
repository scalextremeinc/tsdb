package net.opentsdb.core.sql;

import java.util.Map;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import java.beans.PropertyVetoException;

public final class TsdbSql {
    
    private static final Logger LOG = LoggerFactory.getLogger(TsdbSql.class);
    
    private DataSource ds;
    
    public TsdbSql(String host, String user, String pass, String db) {
        ds = createDataSource(host, user, pass, db);
    }
    
    private DataSource createDataSource(String host, String user, String pass, String db) {
        ComboPooledDataSource cpds = new ComboPooledDataSource();
        try {
            cpds.setDriverClass("com.mysql.jdbc.Driver");
        } catch (PropertyVetoException e) {
            LOG.error("Unable to set dirver class: " + e.getMessage());
        }
        cpds.setJdbcUrl("jdbc:mysql://" + host + "/" + db);
        cpds.setUser(user);
        cpds.setPassword(pass);
        
        // pool settings
        cpds.setMinPoolSize(5);                                     
        cpds.setAcquireIncrement(5);
        cpds.setMaxPoolSize(20);
        
        return cpds;
    }
    
    public void addPoint(String metric, long timestamp, long value, Map<String, String> tags) {
        LOG.error("SQL add point");
        try {
            Connection conn = ds.getConnection();
            
        } catch (SQLException e) {
            LOG.error("Unable to get sql db connection: " + e.getMessage());
        }
    }
    
    public void addPoint(String metric, long timestamp, float value, Map<String, String> tags) {
        LOG.error("SQL add point");
    }
    
}
