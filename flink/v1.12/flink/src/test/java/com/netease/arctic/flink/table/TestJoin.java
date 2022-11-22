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

package com.netease.arctic.flink.table;

import com.netease.arctic.flink.FlinkTestBase;
import com.netease.arctic.flink.extension.MiniClusterExtension;
import com.netease.arctic.flink.util.ArcticUtils;
import com.netease.arctic.flink.util.DataUtil;
import com.netease.arctic.flink.util.TestUtil;
import com.netease.arctic.table.KeyedTable;
import com.netease.arctic.table.TableIdentifier;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;
import org.apache.iceberg.io.TaskWriter;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.netease.arctic.ams.api.MockArcticMetastoreServer.TEST_CATALOG_NAME;
import static com.netease.arctic.table.TableProperties.LOCATION;
import static org.apache.flink.table.planner.factories.TestValuesTableFactory.registerData;

@ExtendWith({MiniClusterExtension.class})
public class TestJoin extends FlinkTestBase {

  public static final Logger LOG = LoggerFactory.getLogger(TestJoin.class);

  private static final String DB = PK_TABLE_ID.getDatabase();
  private static final String TABLE = "test_keyed";

  public void before() throws Exception {
    super.before();
    super.config(TEST_CATALOG_NAME);
  }

  @AfterEach
  public void after() {
    sql("DROP TABLE IF EXISTS arcticCatalog." + DB + "." + TABLE);
  }

  @Timeout(60)
  @Test
  public void testRightEmptyLookupJoin() throws Exception {
    getEnv().getCheckpointConfig().disableCheckpointing();
    List<Object[]> data = new LinkedList<>();
    data.add(new Object[]{RowKind.INSERT, 1000004L, "a", LocalDateTime.now()});
    data.add(new Object[]{RowKind.INSERT, 1000015L, "b", LocalDateTime.now()});
    data.add(new Object[]{RowKind.INSERT, 1000011L, "c", LocalDateTime.now()});
    data.add(new Object[]{RowKind.INSERT, 1000022L, "d", LocalDateTime.now()});
    data.add(new Object[]{RowKind.INSERT, 1000021L, "e", LocalDateTime.now()});
    data.add(new Object[]{RowKind.INSERT, 1000016L, "e", LocalDateTime.now()});
    String id = registerData(DataUtil.toRowList(data));
    sql("CREATE TABLE `user` (id bigint, name string, op_time timestamp(3), watermark for op_time as op_time) " +
        "with (" +
        " 'connector' = 'values'," +
        " 'bounded' = 'false'," +
        " 'data-id' = '" + id + "' " +
        " )");

    sql(String.format("CREATE CATALOG arcticCatalog WITH %s", toWithClause(props)));
    Map<String, String> tableProperties = new HashMap<>();
    tableProperties.put(LOCATION, tableDir.getAbsolutePath() + "/" + TABLE);
    String table = String.format("arcticCatalog.%s.%s", DB, TABLE);

    String sql = String.format("CREATE TABLE IF NOT EXISTS %s (" +
        " info int, id bigint, name STRING" +
        ", PRIMARY KEY (id) NOT ENFORCED) WITH %s", table, toWithClause(tableProperties));
    sql(sql);

    sql("create table d (op_time timestamp(3), watermark for op_time as op_time) like %s", table);

    TableResult result = exec("select u.name, u.id, dim.info, dim.name dname from `user` as u left join d " +
        "/*+OPTIONS('streaming'='true', 'dim-table.enable'='true')*/ for system_time as of u.op_time as dim" +
        " on u.id = dim.id");

    CommonTestUtils.waitUntilJobManagerIsInitialized(() -> result.getJobClient().get().getJobStatus().get());
    Set<Row> actual = new HashSet<>();
    try (CloseableIterator<Row> iterator = result.collect()) {
      for (Object[] datum : data) {
        Row row = iterator.next();
        actual.add(row);
      }
    }
    result.getJobClient().ifPresent(TestUtil::cancelJob);

    List<Object[]> expected = new LinkedList<>();
    expected.add(new Object[]{"a", 1000004L, null, null});
    expected.add(new Object[]{"b", 1000015L, null, null});
    expected.add(new Object[]{"c", 1000011L, null, null});
    expected.add(new Object[]{"d", 1000022L, null, null});
    expected.add(new Object[]{"e", 1000021L, null, null});
    expected.add(new Object[]{"e", 1000016L, null, null});
    Assert.assertEquals(DataUtil.toRowSet(expected), actual);
  }

  @Timeout(60)
  @Test
  public void testLookupJoin() throws Exception {
    getEnv().getCheckpointConfig().disableCheckpointing();
    List<Object[]> data = new LinkedList<>();
    data.add(new Object[]{RowKind.INSERT, 1L, "a", LocalDateTime.now().minusDays(3)});
    data.add(new Object[]{RowKind.INSERT, 2L, "b", LocalDateTime.now()});
    data.add(new Object[]{RowKind.INSERT, 3L, "c", LocalDateTime.now()});
    data.add(new Object[]{RowKind.INSERT, 4L, "d", LocalDateTime.now().plusDays(3)});
    data.add(new Object[]{RowKind.INSERT, 5L, "e", LocalDateTime.now().plusDays(3)});
    data.add(new Object[]{RowKind.INSERT, 3L, "e", LocalDateTime.now()});
    data.add(new Object[]{RowKind.INSERT, 6L, "f", LocalDateTime.now()});
    data.add(new Object[]{RowKind.INSERT, 8L, "g", LocalDateTime.now()});
    data.add(new Object[]{RowKind.INSERT, 9L, "h", LocalDateTime.now()});
    String id = registerData(DataUtil.toRowList(data));
    sql("CREATE TABLE `user` (id bigint, name string, op_time timestamp(3), watermark for op_time as op_time) " +
        "with (" +
        " 'connector' = 'values'," +
        " 'bounded' = 'false'," +
        " 'data-id' = '" + id + "' " +
        " )");

    sql(String.format("CREATE CATALOG arcticCatalog WITH %s", toWithClause(props)));
    Map<String, String> tableProperties = new HashMap<>();
    tableProperties.put(LOCATION, tableDir.getAbsolutePath() + "/" + TABLE);
    String table = String.format("arcticCatalog.%s.%s", DB, TABLE);

    String sql = String.format("CREATE TABLE IF NOT EXISTS %s (" +
        " info int, id bigint, name STRING" +
        ", PRIMARY KEY (id) NOT ENFORCED) WITH %s", table, toWithClause(tableProperties));
    sql(sql);

    TableSchema flinkSchema = TableSchema.builder()
        .field("info", DataTypes.INT())
        .field("id", DataTypes.BIGINT())
        .field("name", DataTypes.STRING())
        .build();
    RowType rowType = (RowType) flinkSchema.toRowDataType().getLogicalType();
    KeyedTable keyedTable = (KeyedTable) ArcticUtils.loadArcticTable(
        ArcticTableLoader.of(TableIdentifier.of(TEST_CATALOG_NAME, DB, TABLE), catalogBuilder));
    TaskWriter<RowData> taskWriter = createKeyedTaskWriter(keyedTable, rowType, 1, true);
    List<RowData> baseData = new ArrayList<RowData>() {{
      add(GenericRowData.ofKind(
          RowKind.INSERT, 123, 1L, StringData.fromString("a")));
      add(GenericRowData.ofKind(
          RowKind.INSERT, 324, 2L, StringData.fromString("b")));
      add(GenericRowData.ofKind(
          RowKind.INSERT, 456, 3L, StringData.fromString("c")));
      add(GenericRowData.ofKind(
          RowKind.INSERT, 463, 4L, StringData.fromString("d")));
    }};
    for (RowData record : baseData) {
      taskWriter.write(record);
    }
    commit(keyedTable, taskWriter.complete(), true);

    writeChange(keyedTable, rowType, 1);

    sql("create table d (op_time timestamp(3), watermark for op_time as op_time) like %s", table);

    TableResult result = exec("select u.name, u.id, dim.info, dim.name dname from `user` as u left join d " +
        "/*+OPTIONS('streaming'='true', 'dim-table.enable'='true')*/ for system_time as of u.op_time as dim" +
        " on u.id = dim.id");

    CommonTestUtils.waitUntilJobManagerIsInitialized(() -> result.getJobClient().get().getJobStatus().get());
    Set<Row> actual = new HashSet<>();
    try (CloseableIterator<Row> iterator = result.collect()) {
      for (Object[] datum : data) {
        Row row = iterator.next();
        actual.add(row);
      }
    }
    result.getJobClient().ifPresent(TestUtil::cancelJob);

    List<Object[]> expected = new LinkedList<>();
    expected.add(new Object[]{"a", 1L, 123, "a"});
    expected.add(new Object[]{"b", 2L, 324, "b"});
    expected.add(new Object[]{"c", 3L, null, null});
    expected.add(new Object[]{"d", 4L, 463, "d"});
    expected.add(new Object[]{"e", 5L, 324, "john"});
    expected.add(new Object[]{"e", 3L, null, null});
    expected.add(new Object[]{"f", 6L, 324, "lily"});
    expected.add(new Object[]{"g", 8L, null, null});
    expected.add(new Object[]{"h", 9L, null, null});
    Assert.assertEquals(DataUtil.toRowSet(expected), actual);
  }

  private void writeChange(KeyedTable keyedTable, RowType rowType, long tranctionId) {
    TaskWriter<RowData> taskWriter = createKeyedTaskWriter(keyedTable, rowType, tranctionId, false);
    List<RowData> data = new ArrayList<RowData>() {{
      add(GenericRowData.ofKind(
          RowKind.INSERT, 324, 5L, StringData.fromString("john")));
      add(GenericRowData.ofKind(
          RowKind.INSERT, 324, 6L, StringData.fromString("lily")));
      add(GenericRowData.ofKind(
          RowKind.DELETE, 324, 3L, StringData.fromString("jake1")));
    }};
    try {
      for (RowData record : data) {
        taskWriter.write(record);
      }
      commit(keyedTable, taskWriter.complete(), false);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
