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

package com.netease.arctic.flink;

import com.netease.arctic.TableTestBase;
import com.netease.arctic.flink.catalog.factories.ArcticCatalogFactoryOptions;
import com.netease.arctic.flink.kafka.testutils.KafkaTestBase;
import com.netease.arctic.flink.write.ArcticRowDataTaskWriterFactory;
import com.netease.arctic.io.reader.GenericArcticDataReader;
import com.netease.arctic.scan.CombinedScanTask;
import com.netease.arctic.scan.KeyedTableScanTask;
import com.netease.arctic.table.KeyedTable;
import com.netease.arctic.table.TableIdentifier;
import com.netease.arctic.table.TableProperties;
import com.netease.arctic.table.UnkeyedTable;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IdentityPartitionConverters;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.MiniClusterResource;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.After;
import org.junit.ClassRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static com.netease.arctic.ams.api.MockArcticMetastoreServer.TEST_CATALOG_NAME;
import static com.netease.arctic.flink.catalog.factories.ArcticCatalogFactoryOptions.IDENTIFIER;
import static org.apache.flink.table.api.config.TableConfigOptions.TABLE_DYNAMIC_TABLE_OPTIONS_ENABLED;

public class FlinkTestBase extends TableTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(FlinkTestBase.class);
  
  @ClassRule
  public static final MiniClusterWithClientResource MINI_CLUSTER_RESOURCE =
      MiniClusterResource.createWithClassloaderCheckDisabled();

  public static boolean IS_LOCAL = true;
  public static String METASTORE_URL = "thrift://127.0.0.1:" + AMS.port();

  public static String metastoreUrl;

  protected static final int KAFKA_PARTITION_NUMS = 1;

  private volatile StreamTableEnvironment tEnv = null;
  protected Map<String, String> props;
  private volatile StreamExecutionEnvironment env = null;

  public static final TableSchema FLINK_SCHEMA = TableSchema.builder()
      .field("id", DataTypes.INT())
      .field("name", DataTypes.STRING())
      .field("op_time", DataTypes.TIMESTAMP())
      .build();
  public static final RowType FLINK_ROW_TYPE = (RowType) FLINK_SCHEMA.toRowDataType().getLogicalType();

  protected static final TableIdentifier PK_TABLE_ID_WITHOUT_PARTITION = TableIdentifier.of(
      TEST_CATALOG_NAME, "test_db", "test_pk_no_partition_table");
  protected static KeyedTable testKeyedNoPartitionTable;

  protected static final TableIdentifier PARTITION_TABLE_ID = TableIdentifier.of(
      TEST_CATALOG_NAME, "test_db", "test_partition_table");
  protected static UnkeyedTable testPartitionTable;

  public static InternalCatalogBuilder catalogBuilder;
  public static final KafkaTestBase kafkaTestBase = new KafkaTestBase();

  public void before() throws Exception {
    if (IS_LOCAL) {
      metastoreUrl = "thrift://127.0.0.1:" + AMS.port();
      testKeyedNoPartitionTable = testCatalog
          .newTableBuilder(PK_TABLE_ID_WITHOUT_PARTITION, TABLE_SCHEMA)
          .withProperty(TableProperties.LOCATION, tableDir.getPath() + "/pk_no_partition_table")
          .withPrimaryKeySpec(PRIMARY_KEY_SPEC)
          .create().asKeyedTable();

      testPartitionTable = testCatalog
          .newTableBuilder(PARTITION_TABLE_ID, TABLE_SCHEMA)
          .withProperty(TableProperties.LOCATION, tableDir.getPath() + "/partition_table")
          .withPartitionSpec(SPEC)
          .create().asUnkeyedTable();

      catalogBuilder = InternalCatalogBuilder.builder().metastoreUrl(metastoreUrl + "/" + TEST_CATALOG_NAME);
    } else {
      metastoreUrl = METASTORE_URL;
    }
  }

  @After
  public void clean() {
    LOG.info("clean start");
    if (IS_LOCAL && testCatalog != null) {
      if (testCatalog.tableExists(PK_TABLE_ID_WITHOUT_PARTITION)) {
        testCatalog.dropTable(PK_TABLE_ID_WITHOUT_PARTITION, true);
        AMS.handler().getTableCommitMetas().remove(PK_TABLE_ID_WITHOUT_PARTITION);
      }

      if (testCatalog.tableExists(PARTITION_TABLE_ID)) {
        testCatalog.dropTable(PARTITION_TABLE_ID, true);
        AMS.handler().getTableCommitMetas().remove(PARTITION_TABLE_ID);
      }
    }
    LOG.info("clean end");
  }

  public void config(String catalog) {
    props = Maps.newHashMap();
    props.put("type", IDENTIFIER);
    props.put(ArcticCatalogFactoryOptions.METASTORE_URL.key(), metastoreUrl + "/" + catalog);
  }

  public static void prepare() throws Exception {
    kafkaTestBase.prepare();
  }

  public static void shutdown() throws Exception {
    kafkaTestBase.shutDownServices();
  }

  protected StreamTableEnvironment getTableEnv() {
    if (tEnv == null) {
      synchronized (this) {
        if (tEnv == null) {
          this.tEnv = StreamTableEnvironment.create(getEnv(), EnvironmentSettings
              .newInstance()
              .inStreamingMode().build());
          Configuration configuration = tEnv.getConfig().getConfiguration();
          // set low-level key-value options
          configuration.setString(TABLE_DYNAMIC_TABLE_OPTIONS_ENABLED.key(), "true");
        }
      }
    }
    return tEnv;
  }

  protected StreamExecutionEnvironment getEnv() {
    if (env == null) {
      synchronized (this) {
        if (env == null) {
          StateBackend backend = new FsStateBackend(
              "file:///" + System.getProperty("java.io.tmpdir") + "/flink/backend");
          env =
              StreamExecutionEnvironment.getExecutionEnvironment(MiniClusterResource.DISABLE_CLASSLOADER_CHECK_CONFIG);
          env.setParallelism(1);
          env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
          env.getCheckpointConfig().setCheckpointInterval(300);
          env.getCheckpointConfig().enableExternalizedCheckpoints(
              CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
          env.setStateBackend(backend);
          env.setRestartStrategy(RestartStrategies.noRestart());
        }
      }
    }
    return env;
  }

  protected List<Row> sql(String query, Object... args) {
    TableResult tableResult = getTableEnv()
        .executeSql(String.format(query, args));
    tableResult.getJobClient().ifPresent(c -> {
      try {
        c.getJobExecutionResult().get();
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    });
    try (CloseableIterator<Row> iter = tableResult.collect()) {
      List<Row> results = Lists.newArrayList(iter);
      return results;
    } catch (Exception e) {
      LOG.warn("Failed to collect table result", e);
      return null;
    }
  }

  protected TableResult exec(String query, Object... args) {
    return exec(getTableEnv(), query, args);
  }

  protected static TableResult exec(TableEnvironment env, String query, Object... args) {
    return env.executeSql(String.format(query, args));
  }

  protected Set<Row> sqlSet(String query, Object... args) {
    return new HashSet<>(sql(query, args));
  }

  public static List<Record> read(KeyedTable table) {
    CloseableIterable<CombinedScanTask> combinedScanTasks = table.newScan().planTasks();
    Schema schema = table.schema();
    GenericArcticDataReader genericArcticDataReader = new GenericArcticDataReader(
        table.io(),
        schema,
        schema,
        table.primaryKeySpec(),
        null,
        true,
        IdentityPartitionConverters::convertConstant
    );
    ImmutableList.Builder<Record> builder = ImmutableList.builder();
    for (CombinedScanTask combinedScanTask : combinedScanTasks) {
      for (KeyedTableScanTask keyedTableScanTask : combinedScanTask.tasks()) {
        builder.addAll(genericArcticDataReader.readData(keyedTableScanTask));
      }
    }
    return builder.build();
  }

  public static Set<Record> toRecords(Collection<Row> rows) {
    GenericRecord record = GenericRecord.create(TABLE_SCHEMA);
    ImmutableSet.Builder<Record> b = ImmutableSet.builder();
    rows.forEach(r ->
        b.add(record.copy(ImmutableMap.of("id", r.getField(0), "name", r.getField(1),
            "op_time", r.getField(2)))));
    return b.build();
  }

  public static String toWithClause(Map<String, String> props) {
    StringBuilder builder = new StringBuilder();
    builder.append("(");
    int propCount = 0;
    for (Map.Entry<String, String> entry : props.entrySet()) {
      if (propCount > 0) {
        builder.append(",");
      }
      builder.append("'").append(entry.getKey()).append("'").append("=")
          .append("'").append(entry.getValue()).append("'");
      propCount++;
    }
    builder.append(")");
    return builder.toString();
  }

  protected static RowData createRowData(Integer id, String name, String dateTime, RowKind rowKind) {
    return GenericRowData.ofKind(rowKind,
        id, StringData.fromString(name), TimestampData.fromLocalDateTime(LocalDateTime.parse(dateTime)));
  }

  protected static RowData createRowData(RowKind rowKind, Object... objects) {
    return GenericRowData.ofKind(
        rowKind, objects[0], StringData.fromString((String) objects[1]),
        TimestampData.fromLocalDateTime((LocalDateTime) objects[2]));
  }

  protected static RowData createRowData(Integer id, String name, String dateTime) {
    return createRowData(id, name, dateTime, RowKind.INSERT);
  }

  protected static void commit(KeyedTable keyedTable, WriteResult result, boolean base) {
    if (base) {
      AppendFiles baseAppend = keyedTable.baseTable().newAppend();
      Arrays.stream(result.dataFiles()).forEach(baseAppend::appendFile);
      baseAppend.commit();
    } else {
      AppendFiles changeAppend = keyedTable.changeTable().newAppend();
      Arrays.stream(result.dataFiles()).forEach(changeAppend::appendFile);
      changeAppend.commit();
    }
  }

  protected static TaskWriter<RowData> createKeyedTaskWriter(KeyedTable keyedTable, RowType rowType, long transactionId,
                                                             boolean base) {
    ArcticRowDataTaskWriterFactory taskWriterFactory =
        new ArcticRowDataTaskWriterFactory(keyedTable, rowType, base);
    taskWriterFactory.setTransactionId(transactionId);
    taskWriterFactory.setMask(3);
    taskWriterFactory.initialize(0, 0);
    return taskWriterFactory.create();
  }
}
