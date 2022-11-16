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

import com.netease.arctic.flink.FlinkTestBase;
import com.netease.arctic.flink.read.hybrid.reader.ReaderFunction;
import com.netease.arctic.flink.read.source.ArcticScanContext;
import com.netease.arctic.flink.table.ArcticTableLoader;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.iceberg.Schema;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static com.netease.arctic.flink.read.ArcticSourceTest.generateRecords;
import static com.netease.arctic.flink.read.ArcticSourceTest.initArcticScanContext;
import static com.netease.arctic.flink.read.ArcticSourceTest.initRowDataReadFunction;
import static com.netease.arctic.flink.read.ArcticSourceTest.triggerFailover;
import static com.netease.arctic.flink.read.hybrid.reader.RowDataReaderFunctionTest.updateRecords;
import static com.netease.arctic.flink.read.hybrid.reader.RowDataReaderFunctionTest.writeUpdate;
import static com.netease.arctic.flink.table.descriptors.ArcticValidator.SCAN_STARTUP_MODE_EARLIEST;

public class DimSourceTest extends FlinkTestBase implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(DimSourceTest.class);
  private static final int PARALLELISM = 4;

  @BeforeClass
  public static void beforeClass() {
    try {
      MINI_CLUSTER_RESOURCE.getMiniCluster().close();
    } catch (Exception e) {
      LOG.info("close mini cluster in base exception.", e);
    }
  }

  @Rule
  public final MiniClusterWithClientResource miniClusterResource =
      new MiniClusterWithClientResource(
          new MiniClusterResourceConfiguration.Builder()
              .setNumberTaskManagers(1)
              .setNumberSlotsPerTaskManager(PARALLELISM)
              .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
              .withHaLeadershipControl()
              .build());

  @Test(timeout = 30000)
  public void testDimTaskManagerFailover() throws Exception {
    List<RowData> updated = updateRecords();
    writeUpdate(updated, testKeyedTable);
    List<RowData> records = generateRecords(2, 1);
    writeUpdate(records, testKeyedTable);

    ArcticSource<RowData> arcticSource = initArcticDimSource(true);
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // enable checkpoint
    env.enableCheckpointing(1000);
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, 0));

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
        ArcticSourceTest.FailoverType.TM,
        jobId,
        WatermarkAwareFailWrapper::continueProcessing,
        miniClusterResource.getMiniCluster());

    while (WatermarkAwareFailWrapper.watermarkCounter.get() != PARALLELISM) {
      Thread.sleep(1000);
      LOG.info("wait for watermark after failover");
    }
    Assert.assertEquals(Long.MAX_VALUE, WatermarkAwareFailWrapper.getWatermarkAfterFailover());
  }

  private ArcticSource<RowData> initArcticDimSource(boolean isStreaming) {
    ArcticTableLoader tableLoader = ArcticTableLoader.of(PK_TABLE_ID, catalogBuilder);
    ArcticScanContext arcticScanContext = initArcticScanContext(isStreaming, SCAN_STARTUP_MODE_EARLIEST);
    ReaderFunction<RowData> rowDataReaderFunction = initRowDataReadFunction(testKeyedTable);
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

    private static void waitToFail() throws ExecutionException, InterruptedException {
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
