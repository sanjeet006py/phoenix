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
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.ConnectionImplementation;
import org.apache.hadoop.hbase.client.MetricsConnection;
import org.apache.hadoop.hbase.client.RegionLocator;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;

import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.BaseTest;
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
    setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
  }

  @Test
  public void testMetaCachingReducesRpcCalls() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    String tableName;

    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      tableName = generateUniqueName();
      StringBuilder splitPoints = new StringBuilder("SPLIT ON (");
      for (int i = 1; i <= 50; i++) {
        if (i > 1) {
          splitPoints.append(", ");
        }
        splitPoints.append(i);
      }
      splitPoints.append(")");
      conn.createStatement().execute("CREATE TABLE " + tableName
          + " (k INTEGER NOT NULL PRIMARY KEY, v VARCHAR) " + splitPoints);

      PreparedStatement upsert =
          conn.prepareStatement("UPSERT INTO " + tableName + " VALUES(?, ?)");
      for (int i = 0; i <= 50; i++) {
        upsert.setInt(1, i);
        upsert.setString(2, "val" + i);
        upsert.execute();
      }
      conn.commit();
    }

    TableName hbaseTableName = TableName.valueOf(tableName);

    Configuration baseConf;
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      baseConf = conn.unwrap(PhoenixConnection.class)
          .getQueryServices().getConfiguration();
    }

    long deltaSmallCaching = measureScanRpcCount(baseConf, hbaseTableName, 5);
    long deltaLargeCaching = measureScanRpcCount(baseConf, hbaseTableName, 100);

    LOGGER.info("Scan RPCs with caching=5: {}, caching=100: {}",
        deltaSmallCaching, deltaLargeCaching);

    assertTrue("Expected more RPCs with smaller caching (caching=5: "
        + deltaSmallCaching + ", caching=100: " + deltaLargeCaching + ")",
        deltaSmallCaching > deltaLargeCaching);
  }

  private long measureScanRpcCount(Configuration baseConf, TableName tableName,
      int metaScannerCaching) throws Exception {
    Configuration conf = new Configuration(baseConf);
    conf.setInt(HConstants.HBASE_META_SCANNER_CACHING, metaScannerCaching);
    conf.setBoolean(MetricsConnection.CLIENT_SIDE_METRICS_ENABLED_KEY, true);

    try (org.apache.hadoop.hbase.client.Connection hbaseConn =
             ConnectionFactory.createConnection(conf)) {

      MetricsConnection metrics =
          ((ConnectionImplementation) hbaseConn).getConnectionMetrics();
      long scansBefore = getScanCallCount(metrics);

      try (RegionLocator locator = hbaseConn.getRegionLocator(tableName)) {
        List<HRegionLocation> locations = locator.getAllRegionLocations();
        LOGGER.info("getAllRegionLocations returned {} locations with caching={}",
            locations.size(), metaScannerCaching);
      }

      long scansAfter = getScanCallCount(metrics);
      return scansAfter - scansBefore;
    }
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
