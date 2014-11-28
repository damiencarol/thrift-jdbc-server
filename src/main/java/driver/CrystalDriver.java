package driver;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;

import driver.io.CCConnection;
import driver.io.ConnectionService;

public class CrystalDriver implements Driver {
    
    final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CrystalDriver.class);
    
    static {
        try {
          java.sql.DriverManager.registerDriver(new CrystalDriver());
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }

    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url))
            return null;

        logger.debug("Call to connect {} {}", url, info);
        try 
        {
            URI r = new URI("thrift:" + url.trim().substring(URL_PREFIX.length()));
            TSocket transport = new TSocket(r.getHost(), r.getPort());
            transport.open();
            
            TBinaryProtocol protocol = new TBinaryProtocol(transport);
            
            ConnectionService.Client client = new ConnectionService.Client(protocol);
            Map<String, String> props = new HashMap<String, String>();
            for (Entry<Object, Object> keyEtr : info.entrySet())
            {
                props.put((String) keyEtr.getKey(), (String) keyEtr.getValue());
            }
            CCConnection conn = client.createConnection(r.getPath(), props);
            return new CrystalConnection(transport, client, conn, url, info);
        } catch (TException e) {
            throw new SQLException(e);
        } catch (URISyntaxException e) {
            throw new SQLException(e);
        }
    }

    private static String URL_PREFIX = "jdbc:crystal:";
    
    public boolean acceptsURL(String url) throws SQLException {
        return Pattern.matches(URL_PREFIX + ".*", url);
    }

    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info)
            throws SQLException {
        logger.debug("Call to DriverPropertyInfo");
        DriverPropertyInfo[] props = new DriverPropertyInfo[] {};
        return props;
    }

    public int getMajorVersion() {
        logger.debug("Call to getMajorVersion");
        return 4;
    }

    public int getMinorVersion() {
        logger.debug("Call to getMinorVersion");
        return 0;
    }

    public boolean jdbcCompliant() {
        logger.debug("Call to jdbcCompliant");
        return true;
    }

    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

}
