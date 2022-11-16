///*
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package com.netease.arctic.flink.read;
//
//import com.netease.arctic.TableTestBase;
//import com.netease.arctic.flink.InternalCatalogBuilder;
//import com.netease.arctic.flink.read.hybrid.enumerator.ContinuousSplitPlannerImplTest;
//import com.netease.arctic.flink.read.hybrid.reader.ReaderFunction;
//import com.netease.arctic.flink.read.hybrid.reader.RowDataReaderFunction;
//import com.netease.arctic.flink.read.hybrid.split.ArcticSplit;
//import com.netease.arctic.flink.read.source.ArcticScanContext;
//import com.netease.arctic.flink.read.source.DataIterator;
//import com.netease.arctic.flink.table.ArcticTableLoader;
//import com.netease.arctic.flink.util.ArcticUtils;
//import com.netease.arctic.flink.write.FlinkSink;
//import com.netease.arctic.table.ArcticTable;
//import com.netease.arctic.table.KeyedTable;
//import com.netease.arctic.table.TableIdentifier;
//import com.netease.arctic.table.TableProperties;
//import org.apache.flink.api.common.JobID;
//import org.apache.flink.api.common.JobStatus;
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.flink.api.common.restartstrategy.RestartStrategies;
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.core.execution.JobClient;
//import org.apache.flink.runtime.highavailability.nonha.embedded.HaLeadershipControl;
//import org.apache.flink.runtime.minicluster.MiniCluster;
//import org.apache.flink.runtime.minicluster.RpcServiceSharing;
//import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.datastream.DataStreamUtils;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.operators.collect.ClientAndIterator;
//import org.apache.flink.table.data.GenericRowData;
//import org.apache.flink.table.data.RowData;
//import org.apache.flink.table.data.StringData;
//import org.apache.flink.table.data.TimestampData;
//import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
//import org.apache.flink.test.util.MiniClusterWithClientResource;
//import org.apache.flink.types.RowKind;
//import org.apache.flink.util.CloseableIterator;
//import org.apache.iceberg.Schema;
//import org.apache.iceberg.flink.FlinkSchemaUtil;
//import org.apache.iceberg.io.TaskWriter;
//import org.junit.Assert;
//import org.junit.ClassRule;
//import org.junit.Test;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.Serializable;
//import java.time.Duration;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.HashSet;
//import java.util.Iterator;
//import java.util.List;
//import java.util.concurrent.CompletableFuture;
//import java.util.concurrent.ExecutionException;
//import java.util.concurrent.atomic.AtomicInteger;
//
//import static com.netease.arctic.ams.api.MockArcticMetastoreServer.TEST_CATALOG_NAME;
//import static com.netease.arctic.ams.api.MockArcticMetastoreServer.TEST_DB_NAME;
//import static com.netease.arctic.flink.FlinkTestBase.FLINK_SCHEMA;
//import static com.netease.arctic.flink.FlinkTestBase.commit;
//import static com.netease.arctic.flink.read.hybrid.enumerator.ContinuousSplitPlannerImplTest.createTaskWriter;
//import static com.netease.arctic.flink.read.hybrid.enumerator.ContinuousSplitPlannerImplTest.ldt;
//import static com.netease.arctic.flink.read.hybrid.reader.RowDataReaderFunctionTest.assertArrayEquals;
//import static com.netease.arctic.flink.read.hybrid.reader.RowDataReaderFunctionTest.excepts;
//import static com.netease.arctic.flink.read.hybrid.reader.RowDataReaderFunctionTest.excepts2;
//import static com.netease.arctic.flink.read.hybrid.reader.RowDataReaderFunctionTest.exceptsCollection;
//import static com.netease.arctic.flink.read.hybrid.reader.RowDataReaderFunctionTest.sortRowDataCollection;
//import static com.netease.arctic.flink.read.hybrid.reader.RowDataReaderFunctionTest.updateRecords;
//import static com.netease.arctic.flink.read.hybrid.reader.RowDataReaderFunctionTest.writeUpdate;
//import static com.netease.arctic.flink.table.descriptors.ArcticValidator.SCAN_STARTUP_MODE_EARLIEST;
//import static com.netease.arctic.flink.table.descriptors.ArcticValidator.SCAN_STARTUP_MODE_LATEST;
//import static org.apache.flink.util.Preconditions.checkArgument;
//import static org.apache.flink.util.Preconditions.checkNotNull;
//import static org.apache.iceberg.flink.MiniClusterResource.DISABLE_CLASSLOADER_CHECK_CONFIG;
//
//public class ArcticSourceTest extends TableTestBase implements Serializable {
//  private static final Logger LOG = LoggerFactory.getLogger(ArcticSourceTest.class);
//  private static final long serialVersionUID = 7418812854449034756L;
//  private static final int PARALLELISM = 4;
//  private InternalCatalogBuilder catalogBuilder;
//  private String metastoreUrl;
//
//  public void before() throws Exception {
//    metastoreUrl = "thrift://127.0.0.1:" + AMS.port();
//    catalogBuilder = InternalCatalogBuilder.builder().metastoreUrl(metastoreUrl + "/" + TEST_CATALOG_NAME);
//
//    ContinuousSplitPlannerImplTest.init(testKeyedTable);
//  }
//
//  @Test(timeout = 30000)
//  public void testArcticSourceStatic() throws Exception {
//    ArcticSource<RowData> arcticSource = initArcticSource(false);
//
//    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//    // enable checkpoint
//    env.enableCheckpointing(3000);
//    // set the source parallelism to 4
//    final CloseableIterator<RowData> resultIterator = env.fromSource(
//        arcticSource,
//        WatermarkStrategy.noWatermarks(),
//        "ArcticParallelSource"
//    ).setParallelism(PARALLELISM).executeAndCollect();
//
//    List<RowData> actualResult = new ArrayList<>();
//
//    resultIterator.forEachRemaining(row -> {
//      GenericRowData rowData = convert(row);
//      actualResult.add(rowData);
//    });
//    Assert.assertEquals(8, actualResult.size());
//    assertArrayEquals(excepts(), actualResult);
//  }
//
//  @Test
//  public void testArcticSourceStaticJobManagerFailover() throws Exception {
//    testArcticSource(FailoverType.JM);
//  }
//
//  @Test
//  public void testArcticSourceStaticTaskManagerFailover() throws Exception {
//    testArcticSource(FailoverType.TM);
//  }
//
//  public void testArcticSource(FailoverType failoverType) throws Exception {
//    List<RowData> expected = new ArrayList<>(exceptsCollection());
//    List<RowData> updated = updateRecords();
//    writeUpdate(updated, testKeyedTable);
//    List<RowData> records = generateRecords(2, 1);
//    writeUpdate(records, testKeyedTable);
//    expected.addAll(updated);
//    expected.addAll(records);
//
//    ArcticSource<RowData> arcticSource = initArcticSource(false);
//    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//    // enable checkpoint
//    env.enableCheckpointing(1000);
//    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));
//
//    DataStream<RowData> input = env.fromSource(
//            arcticSource,
//            WatermarkStrategy.noWatermarks(),
//            "ArcticParallelSource")
//        .setParallelism(PARALLELISM);
//
//    DataStream<RowData> streamFailingInTheMiddleOfReading =
//        RecordCounterToFail.wrapWithFailureAfter(input, expected.size() / 2);
//
//    FlinkSink
//        .forRowData(streamFailingInTheMiddleOfReading)
//        .table(testKeyedTable)
//        .tableLoader(ArcticTableLoader.of(PK_TABLE_ID, catalogBuilder))
//        .flinkSchema(FLINK_SCHEMA)
//        .build();
//
//    JobClient jobClient = env.executeAsync("Bounded Arctic Source Failover Test");
//    JobID jobId = jobClient.getJobID();
//
//    RecordCounterToFail.waitToFail();
//    triggerFailover(
//        failoverType,
//        jobId,
//        RecordCounterToFail::continueProcessing,
//        miniClusterResource.getMiniCluster());
//
//    assertRecords(testKeyedTable, expected, Duration.ofMillis(10), 12000);
//  }
//
//  @Test(timeout = 30000)
//  public void testArcticContinuousSource() throws Exception {
//    ArcticSource<RowData> arcticSource = initArcticSource(true);
//    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//    // enable checkpoint
//    env.enableCheckpointing(1000);
//    ClientAndIterator<RowData> clientAndIterator = executeAndCollectWithClient(env, arcticSource);
//
//    JobClient jobClient = clientAndIterator.client;
//
//    List<RowData> actualResult = collectRecordsFromUnboundedStream(clientAndIterator, excepts().length);
//
//    assertArrayEquals(excepts(), actualResult);
//
//    LOG.info("begin write update_before update_after data and commit new snapshot to change table.");
//    writeUpdate(testKeyedTable);
//
//    actualResult = collectRecordsFromUnboundedStream(clientAndIterator, excepts2().length);
//
//    assertArrayEquals(excepts2(), actualResult);
//    jobClient.cancel();
//  }
//
//  @Test(timeout = 30000)
//  public void testArcticContinuousSourceWithEmptyChangeInInit() throws Exception {
//    TableIdentifier tableId = TableIdentifier.of(TEST_CATALOG_NAME, TEST_DB_NAME, "test_empty_change");
//    KeyedTable table = testCatalog
//        .newTableBuilder(tableId, TABLE_SCHEMA)
//        .withProperty(TableProperties.LOCATION, tableDir.getPath() + "/" + tableId.getTableName())
//        .withPartitionSpec(SPEC)
//        .withPrimaryKeySpec(PRIMARY_KEY_SPEC)
//        .create().asKeyedTable();
//
//    TaskWriter<RowData> taskWriter = createTaskWriter(true, testKeyedTable);
//    List<RowData> baseData = new ArrayList<RowData>() {{
//      add(GenericRowData.ofKind(
//          RowKind.INSERT, 1, StringData.fromString("john"), TimestampData.fromLocalDateTime(ldt)));
//      add(GenericRowData.ofKind(
//          RowKind.INSERT, 2, StringData.fromString("lily"), TimestampData.fromLocalDateTime(ldt)));
//      add(GenericRowData.ofKind(
//          RowKind.INSERT, 3, StringData.fromString("jake"), TimestampData.fromLocalDateTime(ldt.plusDays(1))));
//      add(GenericRowData.ofKind(
//          RowKind.INSERT, 4, StringData.fromString("sam"), TimestampData.fromLocalDateTime(ldt.plusDays(1))));
//    }};
//    for (RowData record : baseData) {
//      taskWriter.write(record);
//    }
//    commit(table, taskWriter.complete(), true);
//
//    ArcticSource<RowData> arcticSource = initArcticSource(true, SCAN_STARTUP_MODE_EARLIEST, tableId);
//    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//    // enable checkpoint
//    env.enableCheckpointing(1000);
//    ClientAndIterator<RowData> clientAndIterator = executeAndCollectWithClient(env, arcticSource);
//
//    JobClient jobClient = clientAndIterator.client;
//
//    List<RowData> actualResult = collectRecordsFromUnboundedStream(clientAndIterator, baseData.size());
//
//    Assert.assertEquals(new HashSet<>(baseData), new HashSet<>(actualResult));
//
//    LOG.info("begin write update_before update_after data and commit new snapshot to change table.");
//    writeUpdate(updateRecords(), table);
//    writeUpdate(updateRecords(), table);
//
//    actualResult = collectRecordsFromUnboundedStream(clientAndIterator, excepts2().length * 2);
//    jobClient.cancel();
//
//    Assert.assertEquals(new HashSet<>(updateRecords()), new HashSet<>(actualResult));
//  }
//
//  @Test
//  public void testLatestStartupMode() throws Exception {
//    ArcticSource<RowData> arcticSource = initArcticSourceWithLatest();
//    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//    // enable checkpoint
//    env.enableCheckpointing(1000);
//
//    ClientAndIterator<RowData> clientAndIterator = executeAndCollectWithClient(env, arcticSource);
//
//    JobClient jobClient = clientAndIterator.client;
//
//    while (true) {
//      if (JobStatus.RUNNING == jobClient.getJobStatus().get()) {
//        Thread.sleep(500);
//        LOG.info("begin write update_before update_after data and commit new snapshot to change table.");
//        writeUpdate(testKeyedTable);
//        break;
//      }
//      Thread.sleep(100);
//    }
//
//    List<RowData> actualResult = collectRecordsFromUnboundedStream(clientAndIterator, excepts2().length);
//
//    assertArrayEquals(excepts2(), actualResult);
//    jobClient.cancel();
//  }
//
//  @Test(timeout = 30000)
//  public void testArcticContinuousSourceJobManagerFailover() throws Exception {
//    testArcticContinuousSource(FailoverType.JM);
//  }
//
//  @Test(timeout = 30000)
//  public void testArcticContinuousSourceTaskManagerFailover() throws Exception {
//    testArcticContinuousSource(FailoverType.TM);
//  }
//
//  public void testArcticContinuousSource(final FailoverType failoverType) throws Exception {
//    List<RowData> expected = new ArrayList<>(Arrays.asList(excepts()));
//    writeUpdate(testKeyedTable);
//    expected.addAll(Arrays.asList(excepts2()));
//
//    ArcticSource<RowData> arcticSource = initArcticSource(true);
//    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//    // enable checkpoint
//    env.enableCheckpointing(1000);
////    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));
//
//    DataStream<RowData> input = env.fromSource(
//            arcticSource,
//            WatermarkStrategy.noWatermarks(),
//            "ArcticParallelSource")
//        .setParallelism(PARALLELISM);
//
//    FlinkSink
//        .forRowData(input)
//        .table(testKeyedTable)
//        .tableLoader(ArcticTableLoader.of(PK_TABLE_ID, catalogBuilder))
//        .flinkSchema(FLINK_SCHEMA)
//        .build();
//
//    JobClient jobClient = env.executeAsync("Unbounded Arctic Source Failover Test");
//    JobID jobId = jobClient.getJobID();
//
//    for (int i = 1; i < 5; i++) {
//      Thread.sleep(10);
//      List<RowData> records = generateRecords(2, i);
//      expected.addAll(records);
//      writeUpdate(records, testKeyedTable);
//      if (i == 2) {
//        triggerFailover(failoverType, jobId, () -> {
//        }, miniClusterResource.getMiniCluster());
//      }
//    }
//
//    // wait longer for continuous source to reduce flakiness
//    // because CI servers tend to be overloaded.
//    assertRecords(testKeyedTable, expected, Duration.ofMillis(10), 12000);
//    jobClient.cancel();
//  }
//
//  private void assertRecords(
//      KeyedTable testFailoverTable, List<RowData> expected, Duration checkInterval, int maxCheckCount)
//      throws InterruptedException {
//    for (int i = 0; i < maxCheckCount; ++i) {
//      if (equalsRecords(expected, tableRecords(testFailoverTable), testFailoverTable.schema())) {
//        break;
//      } else {
//        Thread.sleep(checkInterval.toMillis());
//      }
//    }
//    // success or failure, assert on the latest table state
//    equalsRecords(expected, tableRecords(testFailoverTable), testFailoverTable.schema());
//  }
//
//  private boolean equalsRecords(List<RowData> expected, List<RowData> tableRecords, Schema schema) {
//    try {
//      RowData[] expectedArray = sortRowDataCollection(expected);
//      RowData[] actualArray = sortRowDataCollection(tableRecords);
//      Assert.assertArrayEquals(expectedArray, actualArray);
//      return true;
//    } catch (Throwable e) {
//      return false;
//    }
//  }
//
//  public static List<RowData> tableRecords(final KeyedTable keyedTable) {
//    keyedTable.refresh();
//    List<ArcticSplit> arcticSplits = FlinkSplitPlanner.planFullTable(keyedTable, new AtomicInteger(0));
//
//    RowDataReaderFunction rowDataReaderFunction = new RowDataReaderFunction(
//        new Configuration(),
//        keyedTable.schema(),
//        keyedTable.schema(),
//        keyedTable.primaryKeySpec(),
//        null,
//        true,
//        keyedTable.io()
//    );
//
//    LOG.info("read table. {}", keyedTable);
//    List<RowData> actual = new ArrayList<>();
//    arcticSplits.forEach(split -> {
//      LOG.info("ArcticSplit {}.", split);
//      DataIterator<RowData> dataIterator = rowDataReaderFunction.createDataIterator(split);
//      while (dataIterator.hasNext()) {
//        RowData rowData = dataIterator.next();
//        LOG.info("{}", rowData);
//        actual.add(rowData);
//      }
//    });
//    return actual;
//  }
//
//  public static List<RowData> generateRecords(int numRecords, int index) {
//    int pk = 100;
//    List<RowData> records = new ArrayList<>(numRecords);
//    for (int i = index; i < numRecords + index; i++) {
//      records.add(GenericRowData.ofKind(
//          RowKind.INSERT, pk + index, StringData.fromString("jo" + index + i), TimestampData.fromLocalDateTime(ldt)));
//      records.add(GenericRowData.ofKind(
//          RowKind.DELETE, pk + index, StringData.fromString("jo" + index + i), TimestampData.fromLocalDateTime(ldt)));
//    }
//    return records;
//  }
//
//  // ------------------------------------------------------------------------
//  //  test utilities
//  // ------------------------------------------------------------------------
//
//  public enum FailoverType {
//    NONE,
//    TM,
//    JM
//  }
//
//  public static void triggerFailover(
//      FailoverType type, JobID jobId, Runnable afterFailAction, MiniCluster miniCluster)
//      throws Exception {
//    switch (type) {
//      case NONE:
//        afterFailAction.run();
//        break;
//      case TM:
//        restartTaskManager(afterFailAction, miniCluster);
//        break;
//      case JM:
//        triggerJobManagerFailover(jobId, afterFailAction, miniCluster);
//        break;
//    }
//  }
//
//
//  private static void triggerJobManagerFailover(
//      JobID jobId, Runnable afterFailAction, MiniCluster miniCluster) throws Exception {
//    final HaLeadershipControl haLeadershipControl = miniCluster.getHaLeadershipControl().get();
//    haLeadershipControl.revokeJobMasterLeadership(jobId).get();
//    afterFailAction.run();
//    haLeadershipControl.grantJobMasterLeadership(jobId).get();
//  }
//
//  private static void restartTaskManager(Runnable afterFailAction, MiniCluster miniCluster)
//      throws Exception {
//    miniCluster.terminateTaskManager(0).get();
//    afterFailAction.run();
//    miniCluster.startTaskManager();
//  }
//
//  private List<RowData> collectRecordsFromUnboundedStream(
//      final ClientAndIterator<RowData> client, final int numElements) {
//
//    checkNotNull(client, "client");
//    checkArgument(numElements > 0, "numElement must be > 0");
//
//    final ArrayList<RowData> result = new ArrayList<>(numElements);
//    final Iterator<RowData> iterator = client.iterator;
//
//    while (iterator.hasNext()) {
//      result.add(convert(iterator.next()));
//      if (result.size() == numElements) {
//        return result;
//      }
//    }
//
//    throw new IllegalArgumentException(
//        String.format(
//            "The stream ended before reaching the requested %d records. Only %d records were received.",
//            numElements, result.size()));
//  }
//
//  private ClientAndIterator<RowData> executeAndCollectWithClient(
//      StreamExecutionEnvironment env, ArcticSource<RowData> arcticSource) throws Exception {
//    final DataStreamSource<RowData> source =
//        env.fromSource(
//                arcticSource,
//                WatermarkStrategy.noWatermarks(),
//                "ArcticParallelSource")
//            .setParallelism(PARALLELISM);
//    return DataStreamUtils.collectWithClient(source, "testUpdateSnapshot");
//  }
//
//  private static GenericRowData convert(RowData row) {
//    GenericRowData rowData = new GenericRowData(row.getRowKind(), row.getArity());
//    rowData.setField(0, row.getInt(0));
//    rowData.setField(1, row.getString(1));
//    rowData.setField(2, row.getTimestamp(2, 6));
//    return rowData;
//  }
//
//  private ArcticSource<RowData> initArcticSource(boolean isStreaming) {
//    return initArcticSource(isStreaming, SCAN_STARTUP_MODE_EARLIEST);
//  }
//
//  private ArcticSource<RowData> initArcticSourceWithLatest() {
//    return initArcticSource(true, SCAN_STARTUP_MODE_LATEST);
//  }
//
//  private ArcticSource<RowData> initArcticSource(boolean isStreaming, String scanStartupMode) {
//    ArcticTableLoader tableLoader = initLoader();
//    ArcticScanContext arcticScanContext = initArcticScanContext(isStreaming, scanStartupMode);
//    ReaderFunction<RowData> rowDataReaderFunction = initRowDataReadFunction();
//    TypeInformation<RowData> typeInformation = InternalTypeInfo.of(FlinkSchemaUtil.convert(testKeyedTable.schema()));
//
//    return new ArcticSource<>(
//        tableLoader,
//        arcticScanContext,
//        rowDataReaderFunction,
//        typeInformation,
//        testKeyedTable.name(),
//        false);
//  }
//
//  private ArcticSource<RowData> initArcticSource(boolean isStreaming, String scanStartupMode,
//                                                 TableIdentifier tableIdentifier) {
//    ArcticTableLoader tableLoader = ArcticTableLoader.of(tableIdentifier, catalogBuilder);
//    ArcticScanContext arcticScanContext = initArcticScanContext(isStreaming, scanStartupMode);
//    ArcticTable table = ArcticUtils.loadArcticTable(tableLoader);
//    ReaderFunction<RowData> rowDataReaderFunction = initRowDataReadFunction(table.asKeyedTable());
//    TypeInformation<RowData> typeInformation = InternalTypeInfo.of(FlinkSchemaUtil.convert(table.schema()));
//
//    return new ArcticSource<>(
//        tableLoader,
//        arcticScanContext,
//        rowDataReaderFunction,
//        typeInformation,
//        table.name(),
//        false);
//  }
//
//  private RowDataReaderFunction initRowDataReadFunction() {
//    return initRowDataReadFunction(testKeyedTable);
//  }
//
//  public static RowDataReaderFunction initRowDataReadFunction(KeyedTable keyedTable) {
//    return new RowDataReaderFunction(
//        new Configuration(),
//        keyedTable.schema(),
//        keyedTable.schema(),
//        keyedTable.primaryKeySpec(),
//        null,
//        true,
//        keyedTable.io()
//    );
//  }
//
//  public static ArcticScanContext initArcticScanContext(boolean isStreaming, String scanStartupMode) {
//    return ArcticScanContext.arcticBuilder().streaming(isStreaming).scanStartupMode(scanStartupMode)
//        .monitorInterval(Duration.ofMillis(500)).build();
//  }
//
//  private ArcticTableLoader initLoader() {
//    return ArcticTableLoader.of(PK_TABLE_ID, catalogBuilder);
//  }
//
//  // ------------------------------------------------------------------------
//  //  mini cluster failover utilities
//  // ------------------------------------------------------------------------
//
//  private static class RecordCounterToFail {
//
//    private static AtomicInteger records;
//    private static CompletableFuture<Void> fail;
//    private static CompletableFuture<Void> continueProcessing;
//
//    private static <T> DataStream<T> wrapWithFailureAfter(DataStream<T> stream, int failAfter) {
//
//      records = new AtomicInteger();
//      fail = new CompletableFuture<>();
//      continueProcessing = new CompletableFuture<>();
//      return stream.map(
//          record -> {
//            final boolean halfOfInputIsRead = records.incrementAndGet() > failAfter;
//            final boolean notFailedYet = !fail.isDone();
//            if (notFailedYet && halfOfInputIsRead) {
//              fail.complete(null);
//              continueProcessing.get();
//            }
//            return record;
//          });
//    }
//
//    private static void waitToFail() throws ExecutionException, InterruptedException {
//      fail.get();
//    }
//
//    private static void continueProcessing() {
//      continueProcessing.complete(null);
//    }
//  }
//
//
//}
