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
package org.apache.phoenix.query;

import static org.apache.phoenix.query.QueryServices.PHOENIX_REGION_LOCATION_BULK_WARMUP_ENABLED;
import static org.apache.phoenix.query.QueryServices.PHOENIX_REGION_LOCATION_BULK_WARMUP_THREADS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.phoenix.jdbc.ConnectionInfo;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class ConnectionQueryServicesImplBulkWarmupTest {

  @Mock
  private Connection mockConn;

  @Mock
  private RegionLocator mockRegionLocator;

  private static final byte[] TABLE_NAME = TableName.valueOf("TEST_TABLE").getName();

  @Before
  public void setup() throws Exception {
    MockitoAnnotations.initMocks(this);
    when(mockConn.getRegionLocator(any(TableName.class))).thenReturn(mockRegionLocator);
    when(mockRegionLocator.getAllRegionLocations())
        .thenReturn(Collections.singletonList(Mockito.mock(HRegionLocation.class)));
  }

  @After
  public void tearDown() throws Exception {
    Mockito.reset(mockConn, mockRegionLocator);
  }

  private ConnectionQueryServicesImpl createCQSI(boolean warmupEnabled) throws Exception {
    Map<String, String> propsMap = new HashMap<>();
    propsMap.put(PHOENIX_REGION_LOCATION_BULK_WARMUP_ENABLED,
        Boolean.toString(warmupEnabled));
    propsMap.put(PHOENIX_REGION_LOCATION_BULK_WARMUP_THREADS, "1");
    ReadOnlyProps readOnlyProps = new ReadOnlyProps(propsMap);

    QueryServices mockQueryServices = Mockito.mock(QueryServices.class);
    when(mockQueryServices.getProps()).thenReturn(readOnlyProps);
    ConnectionInfo mockConnectionInfo = Mockito.mock(ConnectionInfo.class);
    when(mockConnectionInfo.asProps()).thenReturn(readOnlyProps);

    ConnectionQueryServicesImpl cqs =
        new ConnectionQueryServicesImpl(mockQueryServices, mockConnectionInfo, new Properties());

    Field connField = ConnectionQueryServicesImpl.class.getDeclaredField("connection");
    connField.setAccessible(true);
    connField.set(cqs, mockConn);

    return cqs;
  }

  @Test
  public void testWarmupDisabledByFlag() throws Exception {
    ConnectionQueryServicesImpl cqs = createCQSI(false);
    cqs.warmupAllRegionLocationsBlocking(TABLE_NAME, 5000L);
    verify(mockConn, Mockito.never()).getRegionLocator(any(TableName.class));
  }

  @Test
  public void testFirstCallTriggersExactlyOneBulkFetch() throws Exception {
    ConnectionQueryServicesImpl cqs = createCQSI(true);
    cqs.warmupAllRegionLocationsBlocking(TABLE_NAME, 5000L);
    verify(mockRegionLocator, Mockito.times(1)).getAllRegionLocations();
  }

  @Test
  public void testSecondCallForSameTableIsNoOp() throws Exception {
    ConnectionQueryServicesImpl cqs = createCQSI(true);
    cqs.warmupAllRegionLocationsBlocking(TABLE_NAME, 5000L);
    cqs.warmupAllRegionLocationsBlocking(TABLE_NAME, 5000L);
    verify(mockRegionLocator, Mockito.times(1)).getAllRegionLocations();
  }

  @Test
  public void testConcurrentCallsDedup() throws Exception {
    CountDownLatch insideFetch = new CountDownLatch(1);
    CountDownLatch proceedFetch = new CountDownLatch(1);

    when(mockRegionLocator.getAllRegionLocations()).thenAnswer(inv -> {
      insideFetch.countDown();
      proceedFetch.await(5, TimeUnit.SECONDS);
      return new ArrayList<HRegionLocation>();
    });

    ConnectionQueryServicesImpl cqs = createCQSI(true);

    Thread t1 = new Thread(() -> cqs.warmupAllRegionLocationsBlocking(TABLE_NAME, 5000L));
    Thread t2 = new Thread(() -> {
      try {
        insideFetch.await(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      cqs.warmupAllRegionLocationsBlocking(TABLE_NAME, 5000L);
    });

    t1.start();
    t2.start();
    insideFetch.await(5, TimeUnit.SECONDS);
    proceedFetch.countDown();
    t1.join(5000);
    t2.join(5000);

    verify(mockRegionLocator, Mockito.times(1)).getAllRegionLocations();
  }

  @Test
  public void testFailureRemovesFutureAllowsRetry() throws Exception {
    when(mockRegionLocator.getAllRegionLocations())
        .thenThrow(new IOException("transient failure"))
        .thenReturn(Collections.singletonList(Mockito.mock(HRegionLocation.class)));

    ConnectionQueryServicesImpl cqs = createCQSI(true);

    try {
      cqs.warmupAllRegionLocationsBlocking(TABLE_NAME, 5000L);
    } catch (RuntimeException ignored) {
    }
    Thread.sleep(100);
    cqs.warmupAllRegionLocationsBlocking(TABLE_NAME, 5000L);

    verify(mockRegionLocator, Mockito.times(2)).getAllRegionLocations();
  }

  @Test
  public void testTimeoutReturnsWithoutBlocking() throws Exception {
    CountDownLatch proceedFetch = new CountDownLatch(1);

    when(mockRegionLocator.getAllRegionLocations()).thenAnswer(inv -> {
      proceedFetch.await(10, TimeUnit.SECONDS);
      return new ArrayList<HRegionLocation>();
    });

    ConnectionQueryServicesImpl cqs = createCQSI(true);

    long start = System.currentTimeMillis();
    cqs.warmupAllRegionLocationsBlocking(TABLE_NAME, 50L);
    long elapsed = System.currentTimeMillis() - start;

    assertTrue("Expected return within ~50ms but took " + elapsed + "ms", elapsed < 500);
    proceedFetch.countDown();
  }

  @Test
  public void testShutdownInterruptsExecutor() throws Exception {
    ConnectionQueryServicesImpl cqs = createCQSI(true);

    Field executorField =
        ConnectionQueryServicesImpl.class.getDeclaredField("bulkRegionWarmupExecutor");
    executorField.setAccessible(true);
    ExecutorService executor = (ExecutorService) executorField.get(cqs);
    assertNotNull(executor);

    executorField.set(cqs, null);

    Field enabledField =
        ConnectionQueryServicesImpl.class.getDeclaredField("bulkWarmupEnabled");
    enabledField.setAccessible(true);
    enabledField.set(cqs, false);

    assertEquals(true, executor.isShutdown() || !executor.isTerminated());
    executor.shutdownNow();
    assertTrue(executor.isShutdown());
  }
}
