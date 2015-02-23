package org.apache.phoenix.end2end.index;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.hbase.index.ipc.PhoenixIndexRpcSchedulerFactory;
import org.apache.phoenix.jdbc.PhoenixTestDriver;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.StringUtil;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import static org.apache.phoenix.util.PhoenixRuntime.*;
import static org.apache.phoenix.util.TestUtil.LOCALHOST;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.*;

/**
 *
 */
@Category(NeedsOwnMiniClusterTest.class)
public class IndexQosIT extends BaseTest {
    private static final Log LOG = LogFactory.getLog(IndexQosIT.class);

    private static String url;
    private static PhoenixTestDriver driver;
    private static HBaseTestingUtility util;

    private static final String SCHEMA_NAME = "S";
    private static final String INDEX_TABLE_NAME = "I";
    private static final String DATA_TABLE_FULL_NAME = SchemaUtil.getTableName(SCHEMA_NAME, "T");
    private static final String INDEX_TABLE_FULL_NAME = SchemaUtil.getTableName(SCHEMA_NAME, "I");

    @Before
    public void doSetup() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        setUpConfigForMiniCluster(conf);
        // custom indexing configs
        conf.setInt("hbase.client.retries.number", 2);
        conf.setInt("hbase.client.pause", 5000);
        conf.setInt("hbase.balancer.period", Integer.MAX_VALUE);
        conf.setLong(QueryServices.INDEX_FAILURE_HANDLING_REBUILD_OVERLAP_TIME_ATTRIB, 0);
        // ensure that the QoS settings are retained
        conf.set("hbase.region.server.rpc.scheduler.factory.class", PhoenixIndexRpcSchedulerFactory.class.getName());

        util = new HBaseTestingUtility(conf);
        util.startMiniCluster();

        // setup the driver
        String clientPort = util.getConfiguration().get(QueryServices.ZOOKEEPER_PORT_ATTRIB);
        url = JDBC_PROTOCOL + JDBC_PROTOCOL_SEPARATOR + LOCALHOST + JDBC_PROTOCOL_SEPARATOR + clientPort
                + JDBC_PROTOCOL_TERMINATOR + PHOENIX_TEST_DRIVER_URL_PARAM;
        driver = initAndRegisterDriver(url, ReadOnlyProps.EMPTY_PROPS);
    }

    @AfterClass
    public static void teardownCluster() throws Exception {
        util.shutdownMiniCluster();
    }

    @Test
    public void writeWithIndexQos() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = driver.connect(url, props);
        conn.setAutoCommit(false);
        // setup the tables
        createTables(conn);

        LOG.info("Jesse - starting to write updates");

        // write some data
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + DATA_TABLE_FULL_NAME + " VALUES(?,?,?)");
        stmt.setString(1, "a");
        stmt.setString(2, "x");
        stmt.setString(3, "1");
        stmt.execute();
        conn.commit();

        stmt = conn.prepareStatement("UPSERT INTO " + DATA_TABLE_FULL_NAME + " VALUES(?,?,?)");
        stmt.setString(1, "a2");
        stmt.setString(2, "x2");
        stmt.setString(3, "2");
        stmt.execute();
        try {
            conn.commit();
        } catch (SQLException e) {}

    }

    private void createTables(Connection conn) throws Exception{
        String query;
        ResultSet rs;

        conn.createStatement().execute(
                "CREATE TABLE " + DATA_TABLE_FULL_NAME + " (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR)");
        query = "SELECT * FROM " + DATA_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertFalse(rs.next());

        conn.createStatement().execute(
                "CREATE INDEX " + INDEX_TABLE_NAME + " ON " + DATA_TABLE_FULL_NAME + " (v1) INCLUDE (v2)");
        query = "SELECT * FROM " + INDEX_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertFalse(rs.next());

        // Verify the metadata for index is correct.
        rs = conn.getMetaData().getTables(null, StringUtil.escapeLike(SCHEMA_NAME), INDEX_TABLE_NAME,
                new String[] { PTableType.INDEX.toString() });
        assertTrue(rs.next());
        assertEquals(INDEX_TABLE_NAME, rs.getString(3));
        assertEquals(PIndexState.ACTIVE.toString(), rs.getString("INDEX_STATE"));
        assertFalse(rs.next());
    }
}
