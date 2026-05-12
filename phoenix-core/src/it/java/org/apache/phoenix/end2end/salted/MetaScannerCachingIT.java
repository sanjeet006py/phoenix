/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.end2end.salted;

import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.ConnectionImplementation;
import org.apache.hadoop.hbase.client.MetricsConnection;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;

import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.ConnectionQueryServicesImpl;
import org.apache.phoenix.query.DelegateConnectionQueryServices;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(NeedsOwnMiniClusterTest.class)
public class MetaScannerCachingIT extends BaseTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(MetaScannerCachingIT.class);

  @BeforeClass
  public static synchronized void doSetup() throws Exception {
    Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
    // Bulk warmup is the Phoenix-API surface that drives a meta scan via
    // RegionLocator.getAllRegionLocations() under the hood. It is gated to
    // salted-table point lookups inside BaseResultIterators.bulkWarmupMetaCache.
    props.put(QueryServices.PHOENIX_REGION_LOCATION_BULK_WARMUP_ENABLED,
        String.valueOf(true));
    setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
  }

  @Test
  public void testMetaCachingReducesRpcCalls() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    String tableName;

    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      tableName = generateUniqueName();
      conn.createStatement().execute("CREATE TABLE " + tableName
          + " (k INTEGER NOT NULL PRIMARY KEY, v VARCHAR) SALT_BUCKETS = 50");

      PreparedStatement upsert =
          conn.prepareStatement("UPSERT INTO " + tableName + " VALUES(?, ?)");
      for (int i = 0; i <= 50; i++) {
        upsert.setInt(1, i);
        upsert.setString(2, "val" + i);
        upsert.execute();
      }
      conn.commit();
    }

    long deltaSmallCaching;
    long deltaLargeCaching;

    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      ConnectionQueryServicesImpl cqsi = unwrapToCQSI(
          conn.unwrap(PhoenixConnection.class).getQueryServices());
      Configuration baseConf = cqsi.getConfiguration();

      deltaSmallCaching = measureSelectScanRpcs(conn, cqsi, baseConf, tableName, 5);
      deltaLargeCaching = measureSelectScanRpcs(conn, cqsi, baseConf, tableName, 100);
    }

    LOGGER.info("Scan RPCs with caching=5: {}, caching=100: {}",
        deltaSmallCaching, deltaLargeCaching);

    assertTrue("Expected more RPCs with smaller caching (caching=5: "
        + deltaSmallCaching + ", caching=100: " + deltaLargeCaching + ")",
        deltaSmallCaching > deltaLargeCaching);
  }

  /**
   * Drives the meta scan through a Phoenix JDBC SELECT -- point lookups on a
   * salted table trigger {@code BaseResultIterators.bulkWarmupMetaCache},
   * which calls CQSI's {@code warmupAllRegionLocationsBlocking}, which in turn
   * calls {@code RegionLocator.getAllRegionLocations()} against CQSI's HBase
   * Connection. To attribute scan RPCs to a specific
   * {@code hbase.meta.scanner.caching} value, we temporarily swap CQSI's HBase
   * Connection with a probe configured for the requested caching, run the
   * Phoenix query, then restore the original.
   */
  private long measureSelectScanRpcs(Connection conn, ConnectionQueryServicesImpl cqsi,
      Configuration baseConf, String tableName, int metaScannerCaching)
      throws Exception {
    Configuration conf = new Configuration(baseConf);
    conf.setInt(HConstants.HBASE_META_SCANNER_CACHING, metaScannerCaching);
    conf.setBoolean(MetricsConnection.CLIENT_SIDE_METRICS_ENABLED_KEY, true);

    Field connField =
        ConnectionQueryServicesImpl.class.getDeclaredField("connection");
    connField.setAccessible(true);
    org.apache.hadoop.hbase.client.Connection original =
        (org.apache.hadoop.hbase.client.Connection) connField.get(cqsi);

    Field warmupsField =
        ConnectionQueryServicesImpl.class.getDeclaredField("bulkRegionWarmups");
    warmupsField.setAccessible(true);
    ConcurrentHashMap<?, ?> warmups =
        (ConcurrentHashMap<?, ?>) warmupsField.get(cqsi);

    try (org.apache.hadoop.hbase.client.Connection probe =
            ConnectionFactory.createConnection(conf)) {
      connField.set(cqsi, probe);
      // Drop any cached completed future so the next SELECT actually runs
      // warmup against the probe.
      warmups.clear();

      MetricsConnection metrics =
          ((ConnectionImplementation) probe).getConnectionMetrics();
      long scansBefore = getScanCallCount(metrics);

      ResultSet rs = conn.createStatement()
          .executeQuery("SELECT * FROM " + tableName
              + " WHERE k IN (1, 5, 9, 13, 17, 23, 29, 37, 41, 47)");
      while (rs.next()) {
      }

      long scansAfter = getScanCallCount(metrics);
      long delta = scansAfter - scansBefore;
      LOGGER.info("Phoenix SELECT with caching={}: scan RPCs={}",
          metaScannerCaching, delta);
      return delta;
    } finally {
      connField.set(cqsi, original);
    }
  }

  private static ConnectionQueryServicesImpl unwrapToCQSI(ConnectionQueryServices svc) {
    while (svc instanceof DelegateConnectionQueryServices) {
      svc = ((DelegateConnectionQueryServices) svc).getDelegate();
    }
    return (ConnectionQueryServicesImpl) svc;
  }

  private long getScanCallCount(MetricsConnection metrics) throws Exception {
    Field scanTrackerField = MetricsConnection.class.getDeclaredField("scanTracker");
    scanTrackerField.setAccessible(true);
    Object scanTracker = scanTrackerField.get(metrics);

    Field callTimerField = scanTracker.getClass().getDeclaredField("callTimer");
    callTimerField.setAccessible(true);
    Object timer = callTimerField.get(scanTracker);
    return (long) timer.getClass().getMethod("getCount").invoke(timer);
  }
}
