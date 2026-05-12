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

import static org.apache.phoenix.monitoring.MetricType.REGION_LOCATION_BULK_WARMUP_ELAPSED_MS;
import static org.apache.phoenix.monitoring.MetricType.REGION_LOCATION_BULK_WARMUP_FAILED_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.REGION_LOCATION_BULK_WARMUP_INVOKED_COUNTER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;

import org.apache.hadoop.hbase.TableName;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.monitoring.MetricType;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.ConnectionQueryServicesImpl;
import org.apache.phoenix.query.DelegateConnectionQueryServices;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(NeedsOwnMiniClusterTest.class)
public class SaltedBulkWarmupIT extends BaseTest {

  @BeforeClass
  public static synchronized void doSetup() throws Exception {
    Map<String, String> props = Maps.newHashMapWithExpectedSize(3);
    props.put(QueryServices.COLLECT_REQUEST_LEVEL_METRICS, String.valueOf(true));
    props.put(QueryServices.PHOENIX_REGION_LOCATION_BULK_WARMUP_ENABLED, String.valueOf(true));
    setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
  }

  @Test
  public void testBulkWarmupInvokedOnSaltedPointLookup() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      String tableName = generateUniqueName();
      conn.createStatement().execute("CREATE TABLE " + tableName
          + " (k INTEGER NOT NULL PRIMARY KEY, v VARCHAR) SALT_BUCKETS = 4");

      PreparedStatement upsert =
          conn.prepareStatement("UPSERT INTO " + tableName + " VALUES(?, ?)");
      for (int i = 1; i <= 10; i++) {
        upsert.setInt(1, i);
        upsert.setString(2, "val" + i);
        upsert.execute();
      }
      conn.commit();

      ResultSet rs = conn.createStatement()
          .executeQuery("SELECT * FROM " + tableName + " WHERE k IN (1, 5, 9)");
      int rowCount = 0;
      while (rs.next()) {
        rowCount++;
      }
      assertEquals(3, rowCount);

      Map<MetricType, Long> metrics = PhoenixRuntime.getOverAllReadRequestMetricInfo(rs);
      assertEquals(1L, (long) metrics.get(REGION_LOCATION_BULK_WARMUP_INVOKED_COUNTER));
      assertEquals(0L, (long) metrics.get(REGION_LOCATION_BULK_WARMUP_FAILED_COUNTER));
      assertTrue(metrics.get(REGION_LOCATION_BULK_WARMUP_ELAPSED_MS) >= 0);
    }
  }

  @Test
  public void testSecondQueryStillReportsInvokedButZeroElapsed() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      String tableName = generateUniqueName();
      conn.createStatement().execute("CREATE TABLE " + tableName
          + " (k INTEGER NOT NULL PRIMARY KEY, v VARCHAR) SALT_BUCKETS = 4");

      PreparedStatement upsert =
          conn.prepareStatement("UPSERT INTO " + tableName + " VALUES(?, ?)");
      for (int i = 1; i <= 10; i++) {
        upsert.setInt(1, i);
        upsert.setString(2, "val" + i);
        upsert.execute();
      }
      conn.commit();

      ResultSet rs1 = conn.createStatement()
          .executeQuery("SELECT * FROM " + tableName + " WHERE k IN (1, 2, 3)");
      while (rs1.next()) {
      }

      ResultSet rs2 = conn.createStatement()
          .executeQuery("SELECT * FROM " + tableName + " WHERE k IN (4, 5, 6)");
      while (rs2.next()) {
      }

      Map<MetricType, Long> metrics = PhoenixRuntime.getOverAllReadRequestMetricInfo(rs2);
      assertEquals(1L, (long) metrics.get(REGION_LOCATION_BULK_WARMUP_INVOKED_COUNTER));
      assertEquals(0L, (long) metrics.get(REGION_LOCATION_BULK_WARMUP_FAILED_COUNTER));
    }
  }

  @Test
  public void testUnsaltedTableDoesNotInvokeWarmup() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      String tableName = generateUniqueName();
      conn.createStatement().execute("CREATE TABLE " + tableName
          + " (k INTEGER NOT NULL PRIMARY KEY, v VARCHAR)");

      PreparedStatement upsert =
          conn.prepareStatement("UPSERT INTO " + tableName + " VALUES(?, ?)");
      for (int i = 1; i <= 5; i++) {
        upsert.setInt(1, i);
        upsert.setString(2, "val" + i);
        upsert.execute();
      }
      conn.commit();

      ResultSet rs = conn.createStatement()
          .executeQuery("SELECT * FROM " + tableName + " WHERE k IN (1, 3, 5)");
      while (rs.next()) {
      }

      Map<MetricType, Long> metrics = PhoenixRuntime.getOverAllReadRequestMetricInfo(rs);
      assertEquals(0L, (long) metrics.get(REGION_LOCATION_BULK_WARMUP_INVOKED_COUNTER));
    }
  }

  @Test
  public void testBulkWarmupFailurePropagatesException() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      String tableName = generateUniqueName();
      conn.createStatement().execute("CREATE TABLE " + tableName
          + " (k INTEGER NOT NULL PRIMARY KEY, v VARCHAR) SALT_BUCKETS = 4");

      ConnectionQueryServicesImpl cqsi = unwrapToCQSI(
          conn.unwrap(PhoenixConnection.class).getQueryServices());

      Field connField =
          ConnectionQueryServicesImpl.class.getDeclaredField("connection");
      connField.setAccessible(true);
      org.apache.hadoop.hbase.client.Connection realHBaseConn =
          (org.apache.hadoop.hbase.client.Connection) connField.get(cqsi);

      Field warmupsField =
          ConnectionQueryServicesImpl.class.getDeclaredField("bulkRegionWarmups");
      warmupsField.setAccessible(true);
      ConcurrentHashMap<?, ?> warmups =
          (ConcurrentHashMap<?, ?>) warmupsField.get(cqsi);

      connField.set(cqsi, null);
      warmups.clear();

      byte[] tableBytes = TableName.valueOf(tableName).getName();
      boolean threw = false;
      try {
        cqsi.warmupAllRegionLocationsBlocking(tableBytes, 5000L);
      } catch (RuntimeException expected) {
        threw = true;
      } finally {
        connField.set(cqsi, realHBaseConn);
      }
      assertTrue("Expected warmup to throw on null connection", threw);
      assertTrue("Expected failed future to be removed from cache",
          warmups.isEmpty());

      warmups.clear();
      cqsi.warmupAllRegionLocationsBlocking(tableBytes, 5000L);
      assertTrue("Expected warmup to succeed after connection restored",
          warmups.containsKey(TableName.valueOf(tableName)));
    }
  }

  private static ConnectionQueryServicesImpl unwrapToCQSI(ConnectionQueryServices svc) {
    while (svc instanceof DelegateConnectionQueryServices) {
      svc = ((DelegateConnectionQueryServices) svc).getDelegate();
    }
    return (ConnectionQueryServicesImpl) svc;
  }
}
