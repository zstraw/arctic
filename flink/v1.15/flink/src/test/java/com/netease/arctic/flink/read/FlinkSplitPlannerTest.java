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

import com.netease.arctic.flink.read.hybrid.enumerator.ContinuousSplitPlannerImplTest;
import com.netease.arctic.flink.read.hybrid.split.ArcticSplit;
import org.apache.iceberg.TableScan;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class FlinkSplitPlannerTest extends ContinuousSplitPlannerImplTest {

  @Test
  public void testPlanSplitFromKeyedTable() {
    testKeyedTable.baseTable().refresh();
    testKeyedTable.changeTable().refresh();
    List<ArcticSplit> splitList = FlinkSplitPlanner.planFullTable(testKeyedTable, new AtomicInteger());
    Assert.assertEquals(7, splitList.size());
  }

  @Test
  public void testIncrementalChangelog() throws IOException {
    testKeyedTable.baseTable().refresh();
    testKeyedTable.changeTable().refresh();
    List<ArcticSplit> splitList = FlinkSplitPlanner.planFullTable(testKeyedTable, new AtomicInteger());

    Assert.assertEquals(7, splitList.size());

    long startSnapshotId = testKeyedTable.changeTable().currentSnapshot().snapshotId();
    writeUpdate();
    testKeyedTable.changeTable().refresh();
    long nowSnapshotId = testKeyedTable.changeTable().currentSnapshot().snapshotId();
    TableScan tableScan = testKeyedTable.changeTable().newScan().appendsBetween(startSnapshotId, nowSnapshotId);

    List<ArcticSplit> changeSplits = FlinkSplitPlanner.planChangeTable(tableScan, new AtomicInteger());

    Assert.assertEquals(1, changeSplits.size());
  }

}