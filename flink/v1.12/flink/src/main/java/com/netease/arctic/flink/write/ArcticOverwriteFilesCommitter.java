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

package com.netease.arctic.flink.write;

import com.netease.arctic.flink.table.ArcticTableLoader;
import com.netease.arctic.flink.util.ArcticUtils;
import com.netease.arctic.op.ArcticOperations;
import com.netease.arctic.op.RewritePartitions;
import com.netease.arctic.table.ArcticTable;
import com.netease.arctic.table.KeyedTable;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

class ArcticOverwriteFilesCommitter extends AbstractStreamOperator<Void>
    implements OneInputStreamOperator<WriteResult, Void>, BoundedOneInput {

  private static final long serialVersionUID = 1L;
  private static final long INITIAL_CHECKPOINT_ID = -1L;
  private static final byte[] EMPTY_MANIFEST_DATA = new byte[0];

  private static final Logger LOG = LoggerFactory.getLogger(ArcticOverwriteFilesCommitter.class);

  // TableLoader to load iceberg table lazily.
  private final ArcticTableLoader tableLoader;
  private final AbstractStreamOperator<Void> icebergFileCommitter;

  // A sorted map to maintain the completed data files for each pending checkpointId (which have not been committed
  // to iceberg table). We need a sorted map here because there's possible that few checkpoints snapshot failed, for
  // example: the 1st checkpoint have 2 data files <1, <file0, file1>>, the 2st checkpoint have 1 data files
  // <2, <file3>>. Snapshot for checkpoint#1 interrupted because of network/disk failure etc, while we don't expect
  // any data loss in iceberg table. So we keep the finished files <1, <file0, file1>> in memory and retry to commit
  // iceberg table when the next checkpoint happen.
  private final NavigableMap<Long, byte[]> dataFilesPerCheckpoint = Maps.newTreeMap();

  private final List<WriteResult> pendingToCommit = Lists.newArrayList();

  // It will have an unique identifier for one job.
  private transient String flinkJobId;
  private transient KeyedTable table;

  ArcticOverwriteFilesCommitter(ArcticTableLoader tableLoader,
                                AbstractStreamOperator<Void> icebergFileCommitter) {
    this.tableLoader = tableLoader;
    this.icebergFileCommitter = icebergFileCommitter;
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
    this.icebergFileCommitter.initializeState(context);
    this.flinkJobId = getContainingTask().getEnvironment().getJobID().toString();

    // Open the table loader and load the table.
    ArcticTable t = ArcticUtils.loadArcticTable(tableLoader);
    Preconditions.checkArgument(t.isKeyedTable(), "table:{} is not a keyed table.", t.name());

  }

  private void commitUpToCheckpoint(NavigableMap<Long, byte[]> deltaManifestsMap,
                                    String newFlinkJobId,
                                    long checkpointId) throws IOException {
    NavigableMap<Long, byte[]> pendingMap = deltaManifestsMap.headMap(checkpointId, true);
    for (Map.Entry<Long, byte[]> e : pendingMap.entrySet()) {
      if (Arrays.equals(EMPTY_MANIFEST_DATA, e.getValue())) {
        // Skip the empty flink manifest.
        continue;
      }

    }

    replacePartitions(pendingResults, newFlinkJobId, checkpointId);
  }

  private void replacePartitions(List<WriteResult> pendingResults, String newFlinkJobId,
                                 long checkpointId) {
    // Partition overwrite does not support delete files.
    int deleteFilesNum = pendingResults.stream().mapToInt(r -> r.deleteFiles().length).sum();
    Preconditions.checkState(deleteFilesNum == 0, "Cannot overwrite partitions with delete files.");

    // Commit the overwrite transaction.
    RewritePartitions rewritePartitions = ArcticOperations.newRewritePartitions(table.asKeyedTable());
    rewritePartitions.withTransactionId(transactionId);

    int numFiles = 0;
    for (WriteResult result : pendingResults.values()) {
      Preconditions.checkState(result.referencedDataFiles().length == 0, "Should have no referenced data files.");

      numFiles += result.dataFiles().length;
      Arrays.stream(result.dataFiles()).forEach(rewritePartitions::addDataFile);
    }

    commitOperation(rewritePartitions, numFiles, 0, "dynamic partition overwrite", newFlinkJobId, checkpointId);
  }

  private void commitOperation(RewritePartitions operation, int numDataFiles, int numDeleteFiles, String description,
                               String newFlinkJobId, long checkpointId) {
    LOG.info("Committing {} with {} data files and {} delete files to table {}", description, numDataFiles,
        numDeleteFiles, table);
    operation.set(FLINK_JOB_ID, newFlinkJobId);

    long start = System.currentTimeMillis();
    operation.commit(); // abort is automatically called if this fails.
    long duration = System.currentTimeMillis() - start;
    LOG.info("Committed in {} ms", duration);
  }

  @Override
  public void processElement(StreamRecord<WriteResult> element) {
    this.pendingToCommit.add(element.getValue());
  }

  @Override
  public void endInput() throws IOException {
    pendingToCommit.clear();

    commitUpToCheckpoint(dataFilesPerCheckpoint, flinkJobId, currentCheckpointId);
  }

  @Override
  public void dispose() throws Exception {
    if (tableLoader != null) {
      tableLoader.close();
    }
  }

}
