package net.opentsdb.core.sql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        
        cpds.setJdbcUrl("jdbc:mysql://" + host + "/" + db);
        cpds.setUser(user);
        cpds.setPassword(pass);
        
        // pool settings
        cpds.setMinPoolSize(5);                                     
        cpds.setAcquireIncrement(5);
        cpds.setMaxPoolSize(20);
        
        return cpds;
    }
    
}
