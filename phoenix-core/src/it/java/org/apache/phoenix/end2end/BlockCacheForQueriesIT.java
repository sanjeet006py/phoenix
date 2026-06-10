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
package org.apache.phoenix.end2end;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.SimpleRegionObserver;
import org.apache.hadoop.hbase.regionserver.ScanOptions;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.phoenix.hbase.index.IndexRegionObserver;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ManualEnvironmentEdge;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

/**
 * End-to-end coverage for {@code phoenix.query.disableBlockCacheForQueries} and the {@code
 * USE_CACHE} hint. These tests observe the {@code cacheBlocks} value on the {@link Scan} objects
 * the RegionServer actually opens, via test region observers, so they prove the server honored the
 * client's preference -- including the derived data-table scan in {@code RegionScannerFactory} that
 * the client never sees, and the §4b write-path row-state scan in {@code IndexRegionObserver}.
 */
@Category(NeedsOwnMiniClusterTest.class)
public class BlockCacheForQueriesIT extends BaseTest {

  @BeforeClass
  public static synchronized void doSetup() throws Exception {
    Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
    // Keep captures deterministic: no background stats scans on the tables under test.
    props.put(QueryServices.STATS_COLLECTION_ENABLED, Boolean.FALSE.toString());
    setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
  }

  @Before
  public void resetObservers() {
    ScanCacheBlocksObserver.reset();
    StoreScanCacheBlocksObserver.reset();
  }

  @After
  public void unsetFailForTesting() {
    IndexRegionObserver.setFailPreIndexUpdatesForTesting(false);
    IndexRegionObserver.setFailDataTableUpdatesForTesting(false);
    IndexRegionObserver.setFailPostIndexUpdatesForTesting(false);
  }

  /**
   * Records the {@code cacheBlocks} value of the live {@link Scan} the RegionServer opens, keyed by
   * region table name. {@code preScannerOpen} fires on the client scanner-open RPC path, so this
   * captures both the client scan (when attached to the queried table) and the cross-region derived
   * data-table scan of an uncovered-index lookup (when attached to the data table).
   */
  public static class ScanCacheBlocksObserver extends SimpleRegionObserver {
    public static final Map<String, Boolean> CACHE_BLOCKS = new ConcurrentHashMap<>();

    public static void reset() {
      CACHE_BLOCKS.clear();
    }

    public static Boolean getCacheBlocks(String tableName) {
      return CACHE_BLOCKS.get(tableName);
    }

    @Override
    public void preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Scan scan) {
      String tableName =
        c.getEnvironment().getRegion().getRegionInfo().getTable().getNameAsString();
      CACHE_BLOCKS.put(tableName, scan.getCacheBlocks());
    }
  }

  /**
   * Records the {@code cacheBlocks} value for every store scanner opened on a region, keyed by
   * table name. {@code preStoreScannerOpen} fires for same-region, server-internal scans (such as
   * {@code IndexRegionObserver.getCurrentRowStates}) that {@code preScannerOpen} never sees.
   * Captures are appended (one per column family per scanner open), so callers reset immediately
   * before the triggering write and scope their assertions to that window.
   */
  public static class StoreScanCacheBlocksObserver extends SimpleRegionObserver {
    public static final Map<String, List<Boolean>> CACHE_BLOCKS = new ConcurrentHashMap<>();

    public static void reset() {
      CACHE_BLOCKS.clear();
    }

    public static List<Boolean> getCacheBlocks(String tableName) {
      return CACHE_BLOCKS.get(tableName);
    }

    @Override
    public void preStoreScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
      ScanOptions options) {
      String tableName =
        c.getEnvironment().getRegion().getRegionInfo().getTable().getNameAsString();
      CACHE_BLOCKS.computeIfAbsent(tableName, k -> new CopyOnWriteArrayList<>())
        .add(options.getScan().getCacheBlocks());
    }
  }

  private Connection getConnection(boolean disableBlockCache, boolean autoCommit, Properties extra)
    throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    props.setProperty(QueryServices.DISABLE_BLOCK_CACHE_FOR_QUERIES_ATTRIB,
      Boolean.toString(disableBlockCache));
    if (extra != null) {
      props.putAll(extra);
    }
    Connection conn = DriverManager.getConnection(getUrl(), props);
    conn.setAutoCommit(autoCommit);
    return conn;
  }

  private static void addObserver(String tableName, Class<?> observerClass) throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      TestUtil.addCoprocessor(conn, tableName, observerClass);
    }
  }

  private static void assertCacheBlocks(String tableName, boolean expected) {
    Boolean actual = ScanCacheBlocksObserver.getCacheBlocks(tableName);
    assertNotNull("No scan was captured on table " + tableName, actual);
    assertEquals("cacheBlocks mismatch on table " + tableName, expected, actual.booleanValue());
  }

  private static void commitWithException(Connection conn) {
    try {
      conn.commit();
      fail("Expected the commit to fail because data-table updates were disabled for testing");
    } catch (Exception e) {
      // expected
    } finally {
      IndexRegionObserver.setFailPostIndexUpdatesForTesting(false);
    }
  }

  // Scenario 1: plain SELECT -- captures the client scan as it arrives post-RPC.
  @Test
  public void testPlainSelect() throws Exception {
    String tableName = generateUniqueName();
    try (Connection conn = getConnection(false, false, null)) {
      conn.createStatement()
        .execute("CREATE TABLE " + tableName + " (id VARCHAR NOT NULL PRIMARY KEY, val VARCHAR)");
      conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES ('a', 'av')");
      conn.commit();
    }
    addObserver(tableName, ScanCacheBlocksObserver.class);

    // Config disabled, plain query -> HBase default (cache blocks).
    runSelectAndAssert(false, "SELECT val FROM " + tableName, tableName, true);
    // Config enabled, plain query -> do not cache blocks.
    runSelectAndAssert(true, "SELECT val FROM " + tableName, tableName, false);
    // Config enabled, USE_CACHE -> force cache blocks.
    runSelectAndAssert(true, "SELECT /*+ USE_CACHE */ val FROM " + tableName, tableName, true);
    // Config enabled, NO_CACHE -> do not cache blocks.
    runSelectAndAssert(true, "SELECT /*+ NO_CACHE */ val FROM " + tableName, tableName, false);
    // Config disabled, USE_CACHE -> cache blocks (observationally a no-op).
    runSelectAndAssert(false, "SELECT /*+ USE_CACHE */ val FROM " + tableName, tableName, true);
    // Config enabled, NO_CACHE wins over USE_CACHE.
    runSelectAndAssert(true, "SELECT /*+ NO_CACHE USE_CACHE */ val FROM " + tableName, tableName,
      false);
  }

  private void runSelectAndAssert(boolean disableBlockCache, String query, String capturedTable,
    boolean expectedCacheBlocks) throws Exception {
    ScanCacheBlocksObserver.reset();
    try (Connection conn = getConnection(disableBlockCache, false, null)) {
      try (ResultSet rs = conn.createStatement().executeQuery(query)) {
        while (rs.next()) {
          // drain the result set so the scan is actually opened on the server
        }
      }
    }
    assertCacheBlocks(capturedTable, expectedCacheBlocks);
  }

  // Scenario 2: uncovered global index SELECT -- the server opens a derived data-table scan
  // (RegionScannerFactory:156). Attach the observer to the DATA table to observe that derived scan.
  @Test
  public void testUncoveredGlobalIndexSelect() throws Exception {
    String dataTableName = generateUniqueName();
    String indexTableName = generateUniqueName();
    try (Connection conn = getConnection(false, false, null)) {
      conn.createStatement().execute("CREATE TABLE " + dataTableName
        + " (id VARCHAR NOT NULL PRIMARY KEY, val1 VARCHAR, val2 VARCHAR)");
      conn.createStatement()
        .execute("CREATE UNCOVERED INDEX " + indexTableName + " ON " + dataTableName + " (val1)");
      conn.createStatement().execute("UPSERT INTO " + dataTableName + " VALUES ('a', 'ab', 'abc')");
      conn.createStatement().execute("UPSERT INTO " + dataTableName + " VALUES ('b', 'bc', 'bcd')");
      conn.commit();
    }
    // Attach to the data table so we capture the derived dataTableScan opened cross-region.
    addObserver(dataTableName, ScanCacheBlocksObserver.class);

    String indexHint = "/*+ INDEX(" + dataTableName + " " + indexTableName + ") ";
    // Config disabled -> derived data-table scan inherits HBase default (cache blocks).
    runUncoveredSelectAndAssert(false,
      "SELECT " + indexHint + "*/ val2 FROM " + dataTableName + " WHERE val1 = 'ab'", dataTableName,
      true);
    // Config enabled -> derived data-table scan does not cache blocks.
    runUncoveredSelectAndAssert(true,
      "SELECT " + indexHint + "*/ val2 FROM " + dataTableName + " WHERE val1 = 'ab'", dataTableName,
      false);
    // Config enabled, USE_CACHE -> derived data-table scan caches blocks.
    runUncoveredSelectAndAssert(true,
      "SELECT " + indexHint + "USE_CACHE */ val2 FROM " + dataTableName + " WHERE val1 = 'ab'",
      dataTableName, true);
    // Config enabled, NO_CACHE -> derived data-table scan does not cache blocks.
    runUncoveredSelectAndAssert(true,
      "SELECT " + indexHint + "NO_CACHE */ val2 FROM " + dataTableName + " WHERE val1 = 'ab'",
      dataTableName, false);
  }

  private void runUncoveredSelectAndAssert(boolean disableBlockCache, String query,
    String dataTableName, boolean expectedCacheBlocks) throws Exception {
    ScanCacheBlocksObserver.reset();
    try (Connection conn = getConnection(disableBlockCache, false, null)) {
      try (ResultSet rs = conn.createStatement().executeQuery(query)) {
        assertTrue("Expected the uncovered-index query to return a row", rs.next());
        assertEquals("abc", rs.getString(1));
      }
    }
    assertCacheBlocks(dataTableName, expectedCacheBlocks);
  }

  // Scenario 3: UPSERT SELECT, server-side path (autoCommit + server mutations + same table).
  @Test
  public void testUpsertSelectServerSide() throws Exception {
    assertUpsertSelectCacheBlocks(true);
  }

  // Scenario 4: UPSERT SELECT, client-side path (autoCommit false pulls the read to the client).
  @Test
  public void testUpsertSelectClientSide() throws Exception {
    assertUpsertSelectCacheBlocks(false);
  }

  private void assertUpsertSelectCacheBlocks(boolean serverSide) throws Exception {
    String tableName = generateUniqueName();
    Properties extra = new Properties();
    extra.setProperty(QueryServices.ENABLE_SERVER_SIDE_UPSERT_MUTATIONS,
      Boolean.toString(serverSide));
    try (Connection conn = getConnection(false, false, null)) {
      conn.createStatement()
        .execute("CREATE TABLE " + tableName + " (id VARCHAR NOT NULL PRIMARY KEY, val VARCHAR)");
      conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES ('a', 'av')");
      conn.commit();
    }
    addObserver(tableName, ScanCacheBlocksObserver.class);

    // Config enabled: the read side of UPSERT SELECT must not cache blocks.
    ScanCacheBlocksObserver.reset();
    try (Connection conn = getConnection(true, serverSide, extra)) {
      conn.createStatement().execute(
        "UPSERT INTO " + tableName + " SELECT 'b', val FROM " + tableName + " WHERE id = 'a'");
      if (!serverSide) {
        conn.commit();
      }
    }
    assertCacheBlocks(tableName, false);

    // Config enabled, USE_CACHE: the read side caches blocks. For UPSERT SELECT, UpsertCompiler
    // builds the read-side plan from the UPSERT statement's hint (it replaces the inner SELECT's
    // hint), so the USE_CACHE hint must be placed on the UPSERT keyword.
    ScanCacheBlocksObserver.reset();
    try (Connection conn = getConnection(true, serverSide, extra)) {
      conn.createStatement().execute("UPSERT /*+ USE_CACHE */ INTO " + tableName + " SELECT 'c', "
        + "val FROM " + tableName + " WHERE id = 'a'");
      if (!serverSide) {
        conn.commit();
      }
    }
    assertCacheBlocks(tableName, true);
  }

  // Scenario 5: DELETE, server-side path.
  @Test
  public void testDeleteServerSide() throws Exception {
    assertDeleteCacheBlocks(true);
  }

  // Scenario 6: DELETE, client-side path.
  @Test
  public void testDeleteClientSide() throws Exception {
    assertDeleteCacheBlocks(false);
  }

  private void assertDeleteCacheBlocks(boolean serverSide) throws Exception {
    String tableName = generateUniqueName();
    Properties extra = new Properties();
    extra.setProperty(QueryServices.ENABLE_SERVER_SIDE_DELETE_MUTATIONS,
      Boolean.toString(serverSide));
    try (Connection conn = getConnection(false, false, null)) {
      conn.createStatement()
        .execute("CREATE TABLE " + tableName + " (id VARCHAR NOT NULL PRIMARY KEY, j INTEGER)");
      conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES ('a', 10)");
      conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES ('b', 20)");
      conn.commit();
    }
    addObserver(tableName, ScanCacheBlocksObserver.class);

    // Config enabled: the DELETE scan must not cache blocks.
    ScanCacheBlocksObserver.reset();
    try (Connection conn = getConnection(true, serverSide, extra)) {
      conn.createStatement().executeUpdate("DELETE FROM " + tableName + " WHERE j = 20");
      if (!serverSide) {
        conn.commit();
      }
    }
    assertCacheBlocks(tableName, false);

    // Config enabled, USE_CACHE: the DELETE scan caches blocks.
    ScanCacheBlocksObserver.reset();
    try (Connection conn = getConnection(true, serverSide, extra)) {
      conn.createStatement()
        .executeUpdate("DELETE /*+ USE_CACHE */ FROM " + tableName + " WHERE j = 10");
      if (!serverSide) {
        conn.commit();
      }
    }
    assertCacheBlocks(tableName, true);
  }

  // Scenario 7: read-repair (GlobalIndexChecker). The read-repair index re-scans are new Scan(scan)
  // copies of the original client scan, so they must honor the client cacheBlocks value -- the
  // data-table rebuild scan is hardened to cacheBlocks=false, but that must not leak onto these.
  @Test
  public void testReadRepairHonorsClientCacheBlocks() throws Exception {
    String dataTableName = generateUniqueName();
    String indexTableName = generateUniqueName();
    try (Connection conn = getConnection(false, false, null)) {
      conn.createStatement().execute("CREATE TABLE " + dataTableName
        + " (id VARCHAR NOT NULL PRIMARY KEY, val1 VARCHAR, val2 VARCHAR)");
      conn.createStatement().execute(
        "CREATE INDEX " + indexTableName + " ON " + dataTableName + " (val1) INCLUDE (val2)");
    }
    // Observe the index table -- the read-repair scanner runs over the (index) client scan, and the
    // read-repair index re-scans are new Scan(scan) copies that inherit its cacheBlocks value.
    addObserver(indexTableName, ScanCacheBlocksObserver.class);
    addObserver(dataTableName, ScanCacheBlocksObserver.class);

    // Config enabled, no hint: the read-repair index scan honors the client's cacheBlocks=false.
    assertReadRepairCacheBlocks(dataTableName, indexTableName, "a", false, false);
    // Config enabled, USE_CACHE: the read-repair index scan honors the client's cacheBlocks=true,
    // proving the maintenance hardening (forced false on the data-table rebuild scan) does not
    // bleed
    // onto the index re-scans, which must follow the client preference.
    assertReadRepairCacheBlocks(dataTableName, indexTableName, "b", true, true);
  }

  private void assertReadRepairCacheBlocks(String dataTableName, String indexTableName,
    String rowSuffix, boolean useCache, boolean expectedCacheBlocks) throws Exception {
    String id = "id_" + rowSuffix;
    String val1 = "v1_" + rowSuffix;
    String val2 = "v2_" + rowSuffix;
    // Leave an UNVERIFIED index row behind (first-phase index write succeeds, data-table write
    // fails), so the next read of this row triggers read-repair.
    try (Connection conn = getConnection(false, false, null)) {
      IndexRegionObserver.setFailPostIndexUpdatesForTesting(true);
      conn.createStatement().execute("UPSERT INTO " + dataTableName + " (id, val1, val2) VALUES ('"
        + id + "', '" + val1 + "', '" + val2 + "')");
      conn.commit();
      IndexRegionObserver.setFailPostIndexUpdatesForTesting(false);
    }

    // Config enabled: the SELECT routes through the index (forced via INDEX hint), triggering
    // read-repair; the index scan must reflect the client's cacheBlocks preference.
    ScanCacheBlocksObserver.reset();
    try (Connection conn = getConnection(true, false, null)) {
      String useCacheHint = useCache ? "USE_CACHE " : "";
      String selectSql = "SELECT /*+ INDEX(" + dataTableName + " " + indexTableName + ") "
        + useCacheHint + "*/ val2 FROM " + dataTableName + " WHERE val1 = '" + val1 + "'";
      try (ResultSet rs = conn.createStatement().executeQuery(selectSql)) {
        // Drain: the unverified row is repaired during the scan.
        while (rs.next()) {
          // no-op
        }
      }
    }
    assertCacheBlocks(indexTableName, expectedCacheBlocks);
    assertCacheBlocks(dataTableName, false);
  }

  // Scenario 8 (bloom branch): IndexRegionObserver.getCurrentRowStates uses the per-key
  // bloom-filter
  // gets when BLOOMFILTER='ROW'. This is a write-path row-state read (run in preBatchMutate while
  // holding row locks).
  @Test
  public void testCurrentRowStateNotCachedWithBloomFilter() throws Exception {
    assertCurrentRowStateNotCachedOnWrite("ROW");
  }

  // Scenario 8 (skipscan branch): with BLOOMFILTER='NONE', getCurrentRowStates uses the SkipScan
  // batch. This is a write-path row-state read.
  @Test
  public void testCurrentRowStateNotCachedWithoutBloomFilter() throws Exception {
    assertCurrentRowStateNotCachedOnWrite("NONE");
  }

  private void assertCurrentRowStateNotCachedOnWrite(String bloomFilter) throws Exception {
    String dataTableName = generateUniqueName();
    String indexTableName = generateUniqueName();
    try (Connection conn = getConnection(false, false, null)) {
      conn.createStatement()
        .execute("CREATE TABLE " + dataTableName
          + " (id VARCHAR NOT NULL PRIMARY KEY, val1 VARCHAR, val2 VARCHAR) BLOOMFILTER='"
          + bloomFilter + "'");
      conn.createStatement().execute(
        "CREATE INDEX " + indexTableName + " ON " + dataTableName + " (val1) INCLUDE (val2)");
      conn.createStatement().execute("UPSERT INTO " + dataTableName + " VALUES ('a', 'ab', 'abc')");
      conn.commit();
    }
    // Observe internal store scanners on the data table.
    addObserver(dataTableName, StoreScanCacheBlocksObserver.class);

    // The atomic upsert drives getCurrentRowStates, which opens the write-path row-state scan.
    StoreScanCacheBlocksObserver.reset();
    try (Connection conn = getConnection(false, true, null)) {
      conn.createStatement().execute("UPSERT INTO " + dataTableName + " VALUES ('a') "
        + "ON DUPLICATE KEY UPDATE val2 = val2 || val2");
    }

    List<Boolean> captured = StoreScanCacheBlocksObserver.getCacheBlocks(dataTableName);
    assertNotNull("No store scan was captured on the data table " + dataTableName, captured);
    assertFalse("Expected at least one captured write-path store scan", captured.isEmpty());
    for (Boolean cacheBlocks : captured) {
      assertFalse(
        "Write-path row-state scan should not cache blocks (bloomFilter=" + bloomFilter + ")",
        cacheBlocks);
    }
  }

  // Scenario 9: ungrouped aggregate (COUNT(*)). UngroupedAggregateRegionObserver wraps the inner
  // scanner without creating a new Scan, so the client scan arrives at preScannerOpen carrying the
  // client's cacheBlocks. Same precedence matrix as the plain SELECT.
  @Test
  public void testUngroupedAggregateHonorsClientCacheBlocks() throws Exception {
    String tableName = generateUniqueName();
    try (Connection conn = getConnection(false, false, null)) {
      conn.createStatement()
        .execute("CREATE TABLE " + tableName + " (id VARCHAR NOT NULL PRIMARY KEY, val VARCHAR)");
      conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES ('a', 'av')");
      conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES ('b', 'bv')");
      conn.commit();
    }
    addObserver(tableName, ScanCacheBlocksObserver.class);

    String query = "SELECT COUNT(*) FROM " + tableName;
    // Config disabled -> HBase default (cache blocks).
    runSelectAndAssert(false, query, tableName, true);
    // Config enabled -> do not cache blocks.
    runSelectAndAssert(true, query, tableName, false);
    // Config enabled, USE_CACHE -> force cache blocks.
    runSelectAndAssert(true, "SELECT /*+ USE_CACHE */ COUNT(*) FROM " + tableName, tableName, true);
    // Config enabled, NO_CACHE -> do not cache blocks.
    runSelectAndAssert(true, "SELECT /*+ NO_CACHE */ COUNT(*) FROM " + tableName, tableName, false);
  }

  // Scenario 10: grouped aggregate (GROUP BY). GroupedAggregateRegionObserver also wraps the inner
  // scanner without creating a new Scan, so the client scan's cacheBlocks reaches preScannerOpen.
  @Test
  public void testGroupedAggregateHonorsClientCacheBlocks() throws Exception {
    String tableName = generateUniqueName();
    try (Connection conn = getConnection(false, false, null)) {
      conn.createStatement().execute("CREATE TABLE " + tableName
        + " (id VARCHAR NOT NULL PRIMARY KEY, grp VARCHAR, val VARCHAR)");
      conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES ('a', 'g1', 'v1')");
      conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES ('b', 'g1', 'v2')");
      conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES ('c', 'g2', 'v3')");
      conn.commit();
    }
    addObserver(tableName, ScanCacheBlocksObserver.class);

    String query = "SELECT grp, COUNT(*) FROM " + tableName + " GROUP BY grp";
    // Config disabled -> HBase default (cache blocks).
    runSelectAndAssert(false, query, tableName, true);
    // Config enabled -> do not cache blocks.
    runSelectAndAssert(true, query, tableName, false);
    // Config enabled, USE_CACHE -> force cache blocks.
    runSelectAndAssert(true,
      "SELECT /*+ USE_CACHE */ grp, COUNT(*) FROM " + tableName + " GROUP BY grp", tableName, true);
    // Config enabled, NO_CACHE -> do not cache blocks.
    runSelectAndAssert(true,
      "SELECT /*+ NO_CACHE */ grp, COUNT(*) FROM " + tableName + " GROUP BY grp", tableName, false);
  }

  // Scenario 11: TTLRegionScanner gap-analysis sub-scan. When a row's live cells span more than the
  // TTL, TTLRegionScanner.isExpired opens a per-window single-row sub-scan (the only scan it sets
  // cacheBlocks on) to detect gaps. That sub-scan opens via region.getScanner on the same region,
  // so it surfaces through preStoreScannerOpen and must inherit the client's cacheBlocks. We force
  // the gap by updating val1 at T0 and val2 at T0+6000 with TTL=5s, then read: val1 (older than the
  // trim boundary) is masked to NULL, which proves the gap-analysis path actually ran.
  @Test
  public void testTTLGapAnalysisScanHonorsClientCacheBlocks() throws Exception {
    String tableName = generateUniqueName();
    try (Connection conn = getConnection(false, false, null)) {
      conn.createStatement().execute("CREATE TABLE " + tableName
        + " (id VARCHAR NOT NULL PRIMARY KEY, val1 VARCHAR, val2 VARCHAR) TTL=5");
    }
    // Attach the observer under the real clock: addObserver issues a modifyTable that reopens the
    // region, and the master-side reopen procedure stalls if the clock is frozen. Captures are
    // still scoped per masking SELECT via the reset in assertTTLGapAnalysisCacheBlocks.
    addObserver(tableName, StoreScanCacheBlocksObserver.class);

    ManualEnvironmentEdge injectEdge = new ManualEnvironmentEdge();
    try {
      try (Connection conn = getConnection(false, false, null)) {
        // Round to a whole second to avoid sub-second timestamp artifacts.
        long t0 = (System.currentTimeMillis() + 1000) / 1000 * 1000;
        injectEdge.setValue(t0);
        EnvironmentEdgeManager.injectEdge(injectEdge);
        conn.createStatement()
          .execute("UPSERT INTO " + tableName + " (id, val1) VALUES ('a', 'x')");
        conn.commit(); // val1@T0, empty@T0
        injectEdge.incrementValue(6000); // T1 = T0 + 6000, a 6s span > 5s TTL
        conn.createStatement()
          .execute("UPSERT INTO " + tableName + " (id, val2) VALUES ('a', 'y')");
        conn.commit(); // val2@T1, empty@T1; leave the edge at T1
      }

      // Config enabled -> gap-analysis sub-scan does not cache blocks.
      assertTTLGapAnalysisCacheBlocks(tableName, true, "", false);
      // Config disabled -> HBase default (cache blocks).
      assertTTLGapAnalysisCacheBlocks(tableName, false, "", true);
      // Config enabled, USE_CACHE -> force cache blocks.
      assertTTLGapAnalysisCacheBlocks(tableName, true, "USE_CACHE ", true);
      // Config enabled, NO_CACHE -> do not cache blocks.
      assertTTLGapAnalysisCacheBlocks(tableName, true, "NO_CACHE ", false);
    } finally {
      EnvironmentEdgeManager.reset();
    }
  }

  private void assertTTLGapAnalysisCacheBlocks(String tableName, boolean disableBlockCache,
    String hint, boolean expectedCacheBlocks) throws Exception {
    // Scope captures to this single masking SELECT.
    StoreScanCacheBlocksObserver.reset();
    try (Connection conn = getConnection(disableBlockCache, false, null)) {
      String sql = "SELECT " + (hint.isEmpty() ? "" : "/*+ " + hint + "*/ ") + "val1, val2 FROM "
        + tableName + " WHERE id = 'a'";
      try (ResultSet rs = conn.createStatement().executeQuery(sql)) {
        assertTrue("Expected the masking SELECT to return row 'a'", rs.next());
        // Proof the gap-analysis path ran: val1@T0 is below the trim boundary and is masked.
        assertNull("Expected val1 to be masked by TTL gap analysis", rs.getString("val1"));
        assertEquals("Expected val2 to survive", "y", rs.getString("val2"));
        assertFalse("Expected exactly one row", rs.next());
      }
    }
    List<Boolean> captured = StoreScanCacheBlocksObserver.getCacheBlocks(tableName);
    assertNotNull("No store scan was captured on the data table " + tableName, captured);
    assertFalse("Expected at least one captured gap-analysis store scan", captured.isEmpty());
    for (Boolean cacheBlocks : captured) {
      assertEquals("Gap-analysis sub-scan cacheBlocks mismatch", expectedCacheBlocks,
        cacheBlocks.booleanValue());
    }
  }
}
