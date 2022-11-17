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

package com.netease.arctic.flink.read;

import com.netease.arctic.catalog.CatalogLoader;
import com.netease.arctic.flink.read.hybrid.reader.ReaderFunction;
import com.netease.arctic.flink.read.hybrid.reader.RowDataReaderFunction;
import com.netease.arctic.flink.read.hybrid.reader.RowDataReaderFunctionTest;
import com.netease.arctic.flink.read.source.ArcticScanContext;
import com.netease.arctic.flink.table.ArcticTableLoader;
import com.netease.arctic.table.KeyedTable;
import com.netease.arctic.table.TableIdentifier;
import com.netease.arctic.table.TableProperties;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.highavailability.nonha.embedded.HaLeadershipControl;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.types.RowKind;
import org.apache.iceberg.Schema;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.netease.arctic.ams.api.MockArcticMetastoreServer.TEST_CATALOG_NAME;
import static com.netease.arctic.ams.api.MockArcticMetastoreServer.TEST_DB_NAME;
import static com.netease.arctic.flink.table.descriptors.ArcticValidator.SCAN_STARTUP_MODE_EARLIEST;

public class ArcticSourceDimTest extends RowDataReaderFunctionTest implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(ArcticSourceDimTest.class);
  private static final long serialVersionUID = 7418812854449034756L;

  protected KeyedTable testFailoverTable;
  protected static final String sinkTableName = "test_sink_exactly_once";
  protected static final TableIdentifier FAIL_TABLE_ID =
      TableIdentifier.of(TEST_CATALOG_NAME, TEST_DB_NAME, sinkTableName);

  @Before
  public void testSetup() throws IOException {
    testCatalog = CatalogLoader.load(AMS.getUrl());

    String db = FAIL_TABLE_ID.getDatabase();
    if (!testCatalog.listDatabases().contains(db)) {
      testCatalog.createDatabase(db);
    }

    if (!testCatalog.tableExists(FAIL_TABLE_ID)) {
      testFailoverTable = testCatalog
          .newTableBuilder(FAIL_TABLE_ID, TABLE_SCHEMA)
          .withProperty(TableProperties.LOCATION, tableDir.getPath() + "/" + sinkTableName)
          .withPartitionSpec(SPEC)
          .withPrimaryKeySpec(PRIMARY_KEY_SPEC)
          .create().asKeyedTable();
    }
  }

  @After
  public void dropTable() {
    testCatalog.dropTable(FAIL_TABLE_ID, true);
  }

  @Test/*(timeout = 30000)*/
  public void testDimTaskManagerFailover() throws Exception {
    List<RowData> updated = updateRecords();
    writeUpdate(updated);
    List<RowData> records = generateRecords(2, 1);
    writeUpdate(records);

    ArcticSource<RowData> arcticSource = initArcticDimSource(true);
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // enable checkpoint
    env.enableCheckpointing(1000);
    env.setRestartStrategy(RestartStrategies.failureRateRestart(100, Time.of(30, TimeUnit.SECONDS), Time.of(0, TimeUnit.SECONDS)));

    DataStream<RowData> input = env.fromSource(
            arcticSource,
            WatermarkStrategy.noWatermarks(),
            "ArcticParallelSource")
        .setParallelism(PARALLELISM);

    WatermarkAwareFailWrapper.wrapWithFailureAfter(input);

    JobClient jobClient = env.executeAsync("Dim Arctic Source Failover Test");
    JobID jobId = jobClient.getJobID();

    WatermarkAwareFailWrapper.waitToFail();
    triggerFailover(
        FailoverType.TM,
        jobId,
        WatermarkAwareFailWrapper::continueProcessing,
        MINI_CLUSTER_RESOURCE.getMiniCluster());

    while (WatermarkAwareFailWrapper.watermarkCounter.get() != PARALLELISM) {
      Thread.sleep(10000);
      LOG.info("wait for watermark after failover");
    }
    Assert.assertEquals(Long.MAX_VALUE, WatermarkAwareFailWrapper.getWatermarkAfterFailover());
  }

  private List<RowData> generateRecords(int numRecords, int index) {
    int pk = 100;
    List<RowData> records = new ArrayList<>(numRecords);
    for (int i = index; i < numRecords + index; i++) {
      records.add(GenericRowData.ofKind(
          RowKind.INSERT, pk + index, StringData.fromString("jo" + index + i), TimestampData.fromLocalDateTime(ldt)));
      records.add(GenericRowData.ofKind(
          RowKind.DELETE, pk + index, StringData.fromString("jo" + index + i), TimestampData.fromLocalDateTime(ldt)));
    }
    return records;
  }

  // ------------------------------------------------------------------------
  //  test utilities
  // ------------------------------------------------------------------------

  private enum FailoverType {
    NONE,
    TM,
    JM
  }

  private static void triggerFailover(
      FailoverType type, JobID jobId, Runnable afterFailAction, MiniCluster miniCluster)
      throws Exception {
    switch (type) {
      case NONE:
        afterFailAction.run();
        break;
      case TM:
        restartTaskManager(afterFailAction, miniCluster);
        break;
      case JM:
        triggerJobManagerFailover(jobId, afterFailAction, miniCluster);
        break;
    }
  }

  private static void triggerJobManagerFailover(
      JobID jobId, Runnable afterFailAction, MiniCluster miniCluster) throws Exception {
    final HaLeadershipControl haLeadershipControl = miniCluster.getHaLeadershipControl().get();
    haLeadershipControl.revokeJobMasterLeadership(jobId).get();
    afterFailAction.run();
    haLeadershipControl.grantJobMasterLeadership(jobId).get();
  }

  private static void restartTaskManager(Runnable afterFailAction, MiniCluster miniCluster)
      throws Exception {
    LOG.info("terminate tm");
    miniCluster.terminateTaskManager(0).get();
    afterFailAction.run();
    miniCluster.startTaskManager();
  }

  private ArcticSource<RowData> initArcticDimSource(boolean isStreaming) {
    ArcticTableLoader tableLoader = initLoader();
    ArcticScanContext arcticScanContext = initArcticScanContext(isStreaming, SCAN_STARTUP_MODE_EARLIEST);
    ReaderFunction<RowData> rowDataReaderFunction = initRowDataReadFunction();
    Schema schema = testKeyedTable.schema();
    Schema schemaWithWm = TypeUtil.join(schema,
        new Schema(Types.NestedField.of(-1, true, "opt", Types.TimestampType.withoutZone())));
    TypeInformation<RowData> typeInformation = InternalTypeInfo.of(FlinkSchemaUtil.convert(schemaWithWm));

    return new ArcticSource<>(
        tableLoader,
        arcticScanContext,
        rowDataReaderFunction,
        typeInformation,
        testKeyedTable.name(),
        true);
  }

  private RowDataReaderFunction initRowDataReadFunction() {
    return initRowDataReadFunction(testKeyedTable);
  }

  private RowDataReaderFunction initRowDataReadFunction(KeyedTable keyedTable) {
    return new RowDataReaderFunction(
        new Configuration(),
        keyedTable.schema(),
        keyedTable.schema(),
        keyedTable.primaryKeySpec(),
        null,
        true,
        keyedTable.io()
    );
  }

  private ArcticScanContext initArcticScanContext(boolean isStreaming, String scanStartupMode) {
    return ArcticScanContext.arcticBuilder().streaming(isStreaming).scanStartupMode(scanStartupMode)
        .monitorInterval(Duration.ofMillis(500)).build();
  }

  private ArcticTableLoader initLoader() {
    return ArcticTableLoader.of(PK_TABLE_ID, catalogBuilder);
  }

  private static class WatermarkAwareFailWrapper {

    private static WatermarkFailoverTestOperator op;
    private static long watermarkAfterFailover = -1;
    private static AtomicInteger watermarkCounter = new AtomicInteger(0);

    public static long getWatermarkAfterFailover() {
      return watermarkAfterFailover;
    }

    private static DataStream<RowData> wrapWithFailureAfter(DataStream<RowData> stream) {
      op = new WatermarkFailoverTestOperator();
      return stream.transform("watermark failover", TypeInformation.of(RowData.class), op);
    }

    private static void waitToFail() throws InterruptedException {
      op.waitToFail();
    }

    private static void continueProcessing() {
      op.continueProcessing();
    }

    static class WatermarkFailoverTestOperator extends AbstractStreamOperator<RowData>
        implements OneInputStreamOperator<RowData, RowData> {

      private static final long serialVersionUID = 1L;
      private static boolean fail = false;
      private static boolean failoverHappened = false;

      public WatermarkFailoverTestOperator() {
        super();
        chainingStrategy = ChainingStrategy.ALWAYS;
      }

      private void waitToFail() throws InterruptedException {
        while (!fail) {
          LOG.info("Waiting to fail");
          Thread.sleep(1000);
        }
      }

      private void continueProcessing() {
        failoverHappened = true;
        LOG.info("failover happened");
      }

      @Override
      public void open() throws Exception {
        super.open();
      }

      @Override
      public void processElement(StreamRecord<RowData> element) throws Exception {
        output.collect(element);
      }

      @Override
      public void processWatermark(Watermark mark) throws Exception {
        LOG.info("processWatermark: {}", mark);
        if (!failoverHappened && mark.getTimestamp() > 0) {
          fail = true;
        }
        if (failoverHappened) {
          LOG.info("failover happened, watermark: {}", mark);
          Assert.assertEquals(Long.MAX_VALUE, mark.getTimestamp());
          if (watermarkAfterFailover == -1) {
            watermarkAfterFailover = mark.getTimestamp();
          } else {
            watermarkAfterFailover = Math.min(watermarkAfterFailover, mark.getTimestamp());
          }
          watermarkCounter.incrementAndGet();
        }
        super.processWatermark(mark);
      }
    }
  }

}
