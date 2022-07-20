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

package com.netease.arctic.flink.read.hybrid.reader;

import com.netease.arctic.data.DataFileType;
import com.netease.arctic.flink.read.FlinkSplitPlanner;
import com.netease.arctic.flink.read.hybrid.split.ArcticSplit;
import com.netease.arctic.flink.read.hybrid.split.ChangelogSplit;
import com.netease.arctic.flink.read.source.DataIterator;
import com.netease.arctic.scan.ArcticFileScanTask;
import com.netease.arctic.scan.BaseArcticFileScanTask;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class ChangelogReadTest extends RowDataReaderFunctionTest{

  public static final Logger LOG = LoggerFactory.getLogger(ChangelogReadTest.class);
  private static final AtomicInteger splitCount = new AtomicInteger();

  @Test
  public void testReadChangelog() throws IOException {

    List<ArcticSplit> arcticSplits = FlinkSplitPlanner.planFullTable(testKeyedTable, new AtomicInteger(0));

    RowDataReaderFunction rowDataReaderFunction = new RowDataReaderFunction(
        new Configuration(),
        testKeyedTable.schema(),
        testKeyedTable.schema(),
        testKeyedTable.primaryKeySpec(),
        null,
        true,
        testKeyedTable.io()
    );

    List<RowData> actual = new ArrayList<>();
    arcticSplits.forEach(split -> {
      LOG.info("ArcticSplit {}.", split);
      DataIterator<RowData> dataIterator = rowDataReaderFunction.createDataIterator(split);
      while (dataIterator.hasNext()) {
        RowData rowData = dataIterator.next();
        LOG.info("{}", rowData);
        actual.add(rowData);
      }
    });

    assertArrayEquals(excepts(), actual);

    long snapshotId = testKeyedTable.changeTable().currentSnapshot().snapshotId();
    writeUpdate(testKeyedTable);
    testKeyedTable.changeTable().refresh();
    long nowSnapshotId = testKeyedTable.changeTable().currentSnapshot().snapshotId();

    CloseableIterable<FileScanTask> changeTasks =
        testKeyedTable.changeTable().newScan().appendsBetween(snapshotId, nowSnapshotId)
            .planFiles();
    CloseableIterator<FileScanTask> iterator = changeTasks.iterator();
    Set<ArcticFileScanTask> appendLogTasks = new HashSet<>();
    Set<ArcticFileScanTask> deleteLogTasks = new HashSet<>();
    while (iterator.hasNext()) {
      FileScanTask fileScanTask = iterator.next();
      BaseArcticFileScanTask task = new BaseArcticFileScanTask(fileScanTask);
      if (task.fileType().equals(DataFileType.INSERT_FILE)) {
        appendLogTasks.add(task);
      } else if (task.fileType().equals(DataFileType.EQ_DELETE_FILE)) {
        deleteLogTasks.add(task);
      } else {
        throw new IllegalArgumentException(
            String.format(
                "DataFileType %s is not supported during change log reading period.",
                task.fileType()));
      }
    }
    ChangelogSplit changelogSplit = new ChangelogSplit(appendLogTasks, deleteLogTasks, splitCount.incrementAndGet());
    LOG.info("ArcticSplit {}.", changelogSplit);
    actual.clear();
    DataIterator<RowData> dataIterator = rowDataReaderFunction.createDataIterator(changelogSplit);
    while (dataIterator.hasNext()) {
      RowData rowData = dataIterator.next();
      LOG.info("{}", rowData);
      actual.add(rowData);
    }
    assertArrayEquals(excepts2(), actual);
  }

}
