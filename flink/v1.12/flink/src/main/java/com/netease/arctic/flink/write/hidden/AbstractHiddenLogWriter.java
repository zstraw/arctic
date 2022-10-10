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

package com.netease.arctic.flink.write.hidden;

import com.netease.arctic.data.ChangeAction;
import com.netease.arctic.flink.shuffle.LogRecordV1;
import com.netease.arctic.flink.shuffle.ShuffleHelper;
import com.netease.arctic.flink.table.ArcticTableLoader;
import com.netease.arctic.flink.util.ArcticUtils;
import com.netease.arctic.flink.write.ArcticLogWriter;
import com.netease.arctic.log.FormatVersion;
import com.netease.arctic.log.LogData;
import com.netease.arctic.log.LogDataJsonSerialization;
import com.netease.arctic.table.ArcticTable;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;

import static com.netease.arctic.flink.util.ArcticUtils.getMaxCommittedTransactionId;
import static org.apache.iceberg.relocated.com.google.common.base.Preconditions.checkNotNull;

/**
 * This is an abstract log queue writer.
 * Sending flip message to the kafka topic when the operator occurs restoring, through the {@link GlobalFlipCommitter}
 * commit {@link GlobalFlipCommitter.CommitRequest} to the jobMaster.
 * {@link this#processElement(StreamRecord)} will process records after all operators has sent flip message to the
 * jobMaster and the jobMaster has finished handling these requests.
 */
public abstract class AbstractHiddenLogWriter extends ArcticLogWriter {
  public static final Logger LOG = LoggerFactory.getLogger(AbstractHiddenLogWriter.class);

  private static final long serialVersionUID = 1L;
  public static final long EPIC_NO_INIT = 0L;
  private int subtaskId;
  private transient ListState<Long> checkpointedState;
  private transient ListState<String> hiddenLogJobIdentifyState;
  private transient ListState<Integer> parallelismState;
  private transient Long ckpComplete;
  private final Schema schema;
  private final Properties producerConfig;
  private final String topic;
  private final ShuffleHelper helper;
  protected final LogMsgFactory<RowData> factory;
  protected LogMsgFactory.Producer<RowData> producer;

  private transient boolean shouldCheckFlipSent = false;
  private transient boolean flipSentSucceed = false;

  private GlobalFlipCommitter flipCommitter;
  private final LogData.FieldGetterFactory<RowData> fieldGetterFactory;
  protected transient LogDataJsonSerialization<RowData> logDataJsonSerialization;

  protected FormatVersion logVersion = FormatVersion.FORMAT_VERSION_V1;
  protected byte[] jobIdentify;
  // store transactionId
  protected long epicNo = EPIC_NO_INIT;
  @Nullable
  protected ArcticTableLoader tableLoader;
  protected transient ArcticTable table;

  protected transient LogData<RowData> logFlip;

  public AbstractHiddenLogWriter(
      Schema schema,
      Properties producerConfig,
      String topic,
      LogMsgFactory<RowData> factory,
      LogData.FieldGetterFactory<RowData> fieldGetterFactory,
      byte[] jobId,
      ShuffleHelper helper,
      @Nullable ArcticTableLoader tableLoader) {
    this.schema = schema;
    this.producerConfig = checkNotNull(producerConfig);
    this.topic = checkNotNull(topic);
    this.factory = factory;
    this.fieldGetterFactory = fieldGetterFactory;
    this.jobIdentify = jobId;
    this.helper = helper;
    this.tableLoader = tableLoader;
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
    if (tableLoader != null) {
      table = ArcticUtils.loadArcticTable(tableLoader);
    }
    subtaskId = getRuntimeContext().getIndexOfThisSubtask();
    checkpointedState =
        context.getOperatorStateStore()
            .getListState(
                new ListStateDescriptor<>(
                    subtaskId + "-task-writer-state",
                    LongSerializer.INSTANCE));

    hiddenLogJobIdentifyState =
        context.getOperatorStateStore()
            .getListState(
                new ListStateDescriptor<>(
                    "hidden-wal-writer-job-identify",
                    StringSerializer.INSTANCE
                ));

    parallelismState =
        context.getOperatorStateStore()
            .getListState(
                new ListStateDescriptor<>(
                    "job-" + Arrays.toString(jobIdentify) + "-parallelism",
                    IntSerializer.INSTANCE));
    // init flip committer function
    flipCommitter =
        new GlobalFlipCommitter(
            getRuntimeContext().getGlobalAggregateManager(),
            new GlobalFlipCommitter.FlipCommitFunction(
                getRuntimeContext().getNumberOfParallelSubtasks(),
                schema,
                fieldGetterFactory,
                factory,
                producerConfig,
                topic,
                helper));
    int parallelism = getRuntimeContext().getNumberOfParallelSubtasks();

    // send flip in init for all task
    epicNo = getCommittedEpicNo(context, parallelism);

    if (context.isRestored() && parallelismSame(parallelism)) {
      jobIdentify = hiddenLogJobIdentifyState.get().iterator().next().getBytes(StandardCharsets.UTF_8);
    } else {
      hiddenLogJobIdentifyState.clear();
      hiddenLogJobIdentifyState.add(new String(jobIdentify, 0, jobIdentify.length, StandardCharsets.UTF_8));
    }

    logFlip = new LogRecordV1(
        logVersion,
        jobIdentify,
        epicNo,
        true,
        ChangeAction.INSERT,
        new GenericRowData(0)
    );
    // signal flip topic
    shouldCheckFlipSent = true;
    flipSentSucceed = flipCommitter.commit(subtaskId, logFlip);

    logDataJsonSerialization = new LogDataJsonSerialization<>(
        checkNotNull(schema),
        checkNotNull(fieldGetterFactory));

    producer =
        factory.createProducer(
            producerConfig,
            topic,
            logDataJsonSerialization,
            helper);

    parallelismState.clear();
    parallelismState.add(parallelism);

    LOG.info(
        "initializeState subtaskId={}, restore={}, lastCkpComplete={}.",
        subtaskId,
        context.isRestored(),
        ckpComplete);

    if (isLogOnly()) {
      epicNo++;
    }
  }

  public boolean isLogOnly() {
    return table == null;
  }
  
  public long getCommittedEpicNo(StateInitializationContext context, int parallelism) throws Exception {
    if (isLogOnly()) {
      // for the case which only write into log.
      if (context.isRestored() && parallelismSame(parallelism)) {
        // get last ckp num from state when failover continuously
        ckpComplete = checkpointedState.get().iterator().next();
        return ckpComplete;
      }
      return EPIC_NO_INIT;
    }
    return getMaxCommittedTransactionId(table, new String(jobIdentify)).orElse(EPIC_NO_INIT);
  }

  @Override
  public void setTransactionId(Long transactionId) {
    this.epicNo = transactionId;
  }

  private boolean parallelismSame(int parallelism) throws Exception {
    if (parallelismState == null ||
        parallelismState.get() == null ||
        !parallelismState.get().iterator().hasNext()) {
      LOG.info("Can't find out parallelism state, ignore sending flips.");
      return false;
    }
    int beforeParallelism =
        parallelismState
            .get()
            .iterator()
            .next();
    if (beforeParallelism != parallelism) {
      LOG.warn(
          "This job restored from state, but has changed parallelism, before:{}, now:{}," +
              " So ignore sending flips now.",
          beforeParallelism, parallelism);
      return false;

    }
    return true;
  }

  @Override
  public void open() throws Exception {
    producer.open();
  }

  public void processElement(StreamRecord<RowData> element) throws Exception {
    int waitCount = 0;
    // this is a sync step that will check sending flip succeed or not
    while (shouldCheckFlip() && !alreadySentFlip()) {
      Thread.sleep(100);
      if (waitCount++ % 100 == 0) {
        LOG.info("Still waiting for sending flip," +
            " while the other subtasks have committed to Global State. this subtask is {}.", subtaskId);
      }
    }
  }

  private boolean alreadySentFlip() throws IOException {
    if (!flipSentSucceed) {
      flipSentSucceed = flipCommitter.hasCommittedFlip(logFlip);
    }
    return flipSentSucceed;
  }

  private boolean shouldCheckFlip() {
    return shouldCheckFlipSent;
  }

  @Override
  public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
    super.prepareSnapshotPreBarrier(checkpointId);
    LOG.info(
        "prepareSnapshotPreBarrier subtaskId={}, checkpointId={}.",
        subtaskId,
        checkpointId);

  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);
    producer.flush();
    LOG.info(
        "snapshotState subtaskId={}, checkpointId={}.",
        subtaskId,
        context.getCheckpointId());
    checkpointedState.clear();
    checkpointedState.add(context.getCheckpointId());
    epicNo = context.getCheckpointId() + 1;
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) throws Exception {
    LOG.info(
        "notifyCheckpointComplete subtaskId={}, checkpointId={}, epicNo={}.",
        subtaskId,
        checkpointId,
        epicNo);
    checkpointedState.clear();
    checkpointedState.add(checkpointId);
  }

  @Override
  public void notifyCheckpointAborted(long checkpointId) throws Exception {
    LOG.info(
        "notifyCheckpointAborted subtaskId={}, checkpointId={}.",
        subtaskId,
        checkpointId);
    if (checkpointedState != null) {
      Iterator<Long> iterator = checkpointedState.get().iterator();
      while (iterator.hasNext()) {
        if (iterator.next() >= checkpointId) {
          iterator.remove();
        }
      }
    }
  }

  @Override
  public void close() throws Exception {
    if (producer != null) {
      producer.close();
    }
  }
}
