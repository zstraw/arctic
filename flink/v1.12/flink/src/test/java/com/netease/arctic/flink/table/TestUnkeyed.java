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

import com.netease.arctic.catalog.ArcticCatalog;
import com.netease.arctic.flink.FlinkTestBase;
import com.netease.arctic.flink.util.DataUtil;
import com.netease.arctic.flink.util.TestUtil;
import com.netease.arctic.flink.util.pulsar.LogPulsarHelper;
import com.netease.arctic.flink.util.pulsar.PulsarTestEnvironment;
import com.netease.arctic.flink.util.pulsar.runtime.PulsarRuntime;
import com.netease.arctic.hive.HiveTableTestBase;
import com.netease.arctic.table.ArcticTable;
import com.netease.arctic.table.TableIdentifier;
import org.apache.flink.table.api.ApiExpression;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static com.netease.arctic.ams.api.MockArcticMetastoreServer.TEST_CATALOG_NAME;
import static com.netease.arctic.table.TableProperties.ENABLE_LOG_STORE;
import static com.netease.arctic.table.TableProperties.LOCATION;
import static com.netease.arctic.table.TableProperties.LOG_STORE_ADDRESS;
import static com.netease.arctic.table.TableProperties.LOG_STORE_MESSAGE_TOPIC;
import static com.netease.arctic.table.TableProperties.LOG_STORE_PROPERTIES_PREFIX;
import static com.netease.arctic.table.TableProperties.LOG_STORE_STORAGE_TYPE_KAFKA;
import static com.netease.arctic.table.TableProperties.LOG_STORE_STORAGE_TYPE_PULSAR;
import static com.netease.arctic.table.TableProperties.LOG_STORE_TYPE;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_ADMIN_URL;

@RunWith(Parameterized.class)
public class TestUnkeyed extends FlinkTestBase {

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();
  @Rule
  public TestName testName = new TestName();
  private static final String TABLE = "test_unkeyed";
  private static final String DB = TABLE_ID.getDatabase();

  private String catalog;
  private ArcticCatalog arcticCatalog;
  private String db;
  private String topic;
  private HiveTableTestBase hiveTableTestBase = new HiveTableTestBase();
  private Map<String, String> tableProperties = new HashMap<>();
  @ClassRule
  public static PulsarTestEnvironment environment = new PulsarTestEnvironment(PulsarRuntime.mock());
  private static LogPulsarHelper pulsarHelper;

  @Parameterized.Parameter
  public boolean isHive;
  @Parameterized.Parameter(1)
  public String logType;

  @Parameterized.Parameters(name = "isHive = {0}, logType = {1}")
  public static Collection parameters() {
    return Arrays.asList(
        new Object[][]{
            {false, LOG_STORE_STORAGE_TYPE_KAFKA},
            {false, LOG_STORE_STORAGE_TYPE_PULSAR}
        });
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    HiveTableTestBase.startMetastore();
    FlinkTestBase.prepare();
    pulsarHelper = new LogPulsarHelper(environment);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    FlinkTestBase.shutdown();
  }

  @Before
  public void init() throws Exception {
    if (isHive) {
      hiveTableTestBase.setupTables();
      arcticCatalog = HiveTableTestBase.hiveCatalog;
    } else {
      arcticCatalog = testCatalog;
    }
  }

  public void before() throws Exception {
    if (isHive) {
      catalog = HiveTableTestBase.HIVE_CATALOG_NAME;
      db = HiveTableTestBase.HIVE_DB_NAME;
    } else {
      catalog = TEST_CATALOG_NAME;
      db = DB;
      super.before();
    }
    prepareLog();

    super.config(catalog);
  }

  private void prepareLog() {
    int i = testName.getMethodName().indexOf("[");
    topic = testName.getMethodName().substring(0, i);
    tableProperties.clear();
    tableProperties.put(ENABLE_LOG_STORE, "true");
    tableProperties.put(LOG_STORE_MESSAGE_TOPIC, topic);

    if (Objects.equals(logType, LOG_STORE_STORAGE_TYPE_PULSAR)) {
      pulsarHelper.op().createTopic(topic, 1);
      tableProperties.put(LOG_STORE_ADDRESS, pulsarHelper.op().serviceUrl());
      tableProperties.put(LOG_STORE_TYPE, LOG_STORE_STORAGE_TYPE_PULSAR);
      tableProperties.put(LOG_STORE_PROPERTIES_PREFIX + PULSAR_ADMIN_URL.key(), pulsarHelper.op().adminUrl());
    } else {
      kafkaTestBase.createTopics(KAFKA_PARTITION_NUMS, topic);
      tableProperties.put(LOG_STORE_TYPE, LOG_STORE_STORAGE_TYPE_KAFKA);
      tableProperties.put(LOG_STORE_ADDRESS, kafkaTestBase.brokerConnectionStrings);
    }
  }

  @After
  public void after() {
    sql("DROP TABLE IF EXISTS arcticCatalog." + db + "." + TABLE);
    if (isHive) {
      hiveTableTestBase.clearTable();
    }
    if (Objects.equals(logType, LOG_STORE_STORAGE_TYPE_PULSAR)) {
      pulsarHelper.op().deleteTopicByForce(topic);
    }
  }

  @Test
  public void testUnPartitionDDL() throws IOException {
    sql("CREATE CATALOG arcticCatalog WITH %s", toWithClause(props));

    sql("CREATE TABLE IF NOT EXISTS arcticCatalog." + db + "." + TABLE + "(" +
        " id INT, name STRING, age SMALLINT, sex TINYINT, score BIGINT, height FLOAT, speed DOUBLE, ts TIMESTAMP)" +
        " WITH (" +
        " 'location' = '" + tableDir.getAbsolutePath() + "/" + TABLE + "'" +
        ")");

    ArcticTable table = arcticCatalog.loadTable(TableIdentifier.of(catalog, db, TABLE));

    Schema required = new Schema(
        Types.NestedField.optional(1, "id", Types.IntegerType.get()),
        Types.NestedField.optional(2, "name", Types.StringType.get()),
        Types.NestedField.optional(3, "age", Types.IntegerType.get()),
        Types.NestedField.optional(4, "sex", Types.IntegerType.get()),
        Types.NestedField.optional(5, "score", Types.LongType.get()),
        Types.NestedField.optional(6, "height", Types.FloatType.get()),
        Types.NestedField.optional(7, "speed", Types.DoubleType.get()),
        Types.NestedField.optional(8, "ts", Types.TimestampType.withoutZone())
    );
    Assert.assertEquals(required.asStruct(), table.schema().asStruct());
  }

  @Test
  public void testPartitionDDL() throws IOException {
    sql("CREATE CATALOG arcticCatalog WITH %s", toWithClause(props));

    sql("CREATE TABLE IF NOT EXISTS arcticCatalog." + db + "." + TABLE + "(" +
        " id INT, name STRING, age SMALLINT, sex TINYINT, score BIGINT, height FLOAT, speed DOUBLE, ts TIMESTAMP)" +
        " PARTITIONED BY (ts)" +
        " WITH (" +
        " 'location' = '" + tableDir.getAbsolutePath() + "/" + TABLE + "'" +
        ")");

    Schema required = new Schema(
        Types.NestedField.optional(1, "id", Types.IntegerType.get()),
        Types.NestedField.optional(2, "name", Types.StringType.get()),
        Types.NestedField.optional(3, "age", Types.IntegerType.get()),
        Types.NestedField.optional(4, "sex", Types.IntegerType.get()),
        Types.NestedField.optional(5, "score", Types.LongType.get()),
        Types.NestedField.optional(6, "height", Types.FloatType.get()),
        Types.NestedField.optional(7, "speed", Types.DoubleType.get()),
        Types.NestedField.optional(8, "ts", Types.TimestampType.withoutZone())
    );
    ArcticTable table = arcticCatalog.loadTable(TableIdentifier.of(catalog, db, TABLE));
    Assert.assertEquals(required.asStruct(), table.schema().asStruct());

    PartitionSpec requiredSpec = PartitionSpec.builderFor(required).identity("ts").build();
    Assert.assertEquals(requiredSpec, table.spec());
  }

  @Test
  public void testSinkBatchRead() throws IOException {
    List<Object[]> data = new LinkedList<>();
    data.add(new Object[]{1000004, "a", LocalDateTime.parse("2022-06-17T10:10:11.0")});
    data.add(new Object[]{1000015, "b", LocalDateTime.parse("2022-06-17T10:08:11.0")});
    data.add(new Object[]{1000011, "c", LocalDateTime.parse("2022-06-18T10:10:11.0")});
    data.add(new Object[]{1000014, "d", LocalDateTime.parse("2022-06-17T10:11:11.0")});
    data.add(new Object[]{1000021, "d", LocalDateTime.parse("2022-06-17T16:10:11.0")});
    data.add(new Object[]{1000007, "e", LocalDateTime.parse("2022-06-17T10:10:11.0")});

    List<ApiExpression> rows = DataUtil.toRows(data);

    Table input = getTableEnv().fromValues(DataTypes.ROW(
            DataTypes.FIELD("id", DataTypes.INT()),
            DataTypes.FIELD("name", DataTypes.STRING()),
            DataTypes.FIELD("op_time", DataTypes.TIMESTAMP())
        ),
        rows
    );
    getTableEnv().createTemporaryView("input", input);

    sql("CREATE CATALOG arcticCatalog WITH %s", toWithClause(props));
    sql("CREATE TABLE IF NOT EXISTS arcticCatalog." + db + "." + TABLE + "(" +
        " id INT, name STRING, op_time TIMESTAMP)" +
        " WITH (" +
        " 'location' = '" + tableDir.getAbsolutePath() + "/" + TABLE + "'" +
        ")");

    sql("insert into arcticCatalog." + db + "." + TABLE +
        "/*+ OPTIONS('arctic.emit.mode'='file')*/ select * from input");

    ArcticTable table = arcticCatalog.loadTable(TableIdentifier.of(catalog, db, TABLE));
    Iterable<Snapshot> snapshots = table.asUnkeyedTable().snapshots();
    Snapshot s = snapshots.iterator().next();

    Assert.assertEquals(
        DataUtil.toRowSet(data), new HashSet<>(sql("select * from arcticCatalog." + db + "." + TABLE +
            "/*+ OPTIONS(" +
            "'arctic.read.mode'='file'" +
            ", 'streaming'='false'" +
            ", 'snapshot-id'='" + s.snapshotId() + "'" +
            ")*/" +
            "")));
  }

  @Test
  public void testSinkStreamRead() throws Exception {
    List<Object[]> data = new LinkedList<>();
    data.add(new Object[]{1000004, "a"});
    data.add(new Object[]{1000015, "b"});
    data.add(new Object[]{1000011, "c"});
    data.add(new Object[]{1000014, "d"});
    data.add(new Object[]{1000021, "d"});
    data.add(new Object[]{1000007, "e"});

    List<ApiExpression> rows = DataUtil.toRows(data);

    Table input = getTableEnv().fromValues(DataTypes.ROW(
            DataTypes.FIELD("id", DataTypes.INT()),
            DataTypes.FIELD("name", DataTypes.STRING())
        ),
        rows
    );
    getTableEnv().createTemporaryView("input", input);

    sql("CREATE CATALOG arcticCatalog WITH %s", toWithClause(props));
    sql("CREATE TABLE IF NOT EXISTS arcticCatalog." + db + "." + TABLE + "(id INT, name STRING) " +
        " WITH (" +
        " 'location' = '" + tableDir.getAbsolutePath() + "/" + TABLE + "'" +
        ")");

    sql("insert into arcticCatalog." + db + "." + TABLE + " select * from input");
    sql("insert into arcticCatalog." + db + "." + TABLE + " select * from input");

    ArcticTable table = arcticCatalog.loadTable(TableIdentifier.of(catalog, db, TABLE));

    Iterable<Snapshot> snapshots = table.asUnkeyedTable().snapshots();
    Snapshot s = snapshots.iterator().next();

    TableResult result = exec("select * from arcticCatalog." + db + "." + TABLE +
        "/*+ OPTIONS(" +
        "'arctic.read.mode'='file'" +
        ", 'start-snapshot-id'='" + s.snapshotId() + "'" +
        ")*/" +
        "");

    Set<Row> actual = new HashSet<>();
    try (CloseableIterator<Row> iterator = result.collect()) {
      for (int i = 0; i < data.size(); i++) {
        actual.add(iterator.next());
      }
    }
    result.getJobClient().ifPresent(TestUtil::cancelJob);
    Assert.assertEquals(DataUtil.toRowSet(data), actual);
  }

  @Test
  public void testLogSinkSource() throws Exception {
    List<Object[]> data = new LinkedList<>();
    data.add(new Object[]{1000004, "a"});
    data.add(new Object[]{1000015, "b"});
    data.add(new Object[]{1000011, "c"});
    data.add(new Object[]{1000014, "d"});
    data.add(new Object[]{1000021, "d"});
    data.add(new Object[]{1000007, "e"});

    List<ApiExpression> rows = DataUtil.toRows(data);
    Table input = getTableEnv().fromValues(DataTypes.ROW(
            DataTypes.FIELD("id", DataTypes.INT()),
            DataTypes.FIELD("name", DataTypes.STRING())
        ),
        rows
    );
    getTableEnv().createTemporaryView("input", input);

    sql("CREATE CATALOG arcticCatalog WITH %s", toWithClause(props));

    tableProperties.put(LOCATION, tableDir.getAbsolutePath() + "/" + TABLE);
    sql("CREATE TABLE IF NOT EXISTS arcticCatalog." + db + "." + TABLE + "(" +
        " id INT, name STRING) WITH %s", toWithClause(tableProperties));

    sql("insert into arcticCatalog." + db + "." + TABLE + " /*+ OPTIONS(" +
        "'arctic.emit.mode'='log'" +
        ", 'log.version'='v1'" +
        ") */" +
        " select * from input");

    TableResult result = exec("select * from arcticCatalog." + db + "." + TABLE +
        "/*+ OPTIONS(" +
        "'arctic.read.mode'='log'" +
        ", 'scan.startup.mode'='earliest'" +
        ")*/" +
        "");

    Set<Row> actual = new HashSet<>();
    try (CloseableIterator<Row> iterator = result.collect()) {
      for (Object[] datum : data) {
        actual.add(iterator.next());
      }
    }
    Assert.assertEquals(DataUtil.toRowSet(data), actual);

    result.getJobClient().ifPresent(TestUtil::cancelJob);
  }

  @Test
  public void testUnPartitionDoubleSink() throws Exception {
    List<Object[]> data = new LinkedList<>();
    data.add(new Object[]{1000004, "a"});
    data.add(new Object[]{1000015, "b"});
    data.add(new Object[]{1000011, "c"});
    data.add(new Object[]{1000014, "d"});
    data.add(new Object[]{1000021, "d"});
    data.add(new Object[]{1000007, "e"});

    List<ApiExpression> rows = DataUtil.toRows(data);
    Table input = getTableEnv().fromValues(DataTypes.ROW(
            DataTypes.FIELD("id", DataTypes.INT()),
            DataTypes.FIELD("name", DataTypes.STRING())
        ),
        rows
    );
    getTableEnv().createTemporaryView("input", input);
    sql("CREATE CATALOG arcticCatalog WITH %s", toWithClause(props));

    tableProperties.put(LOCATION, tableDir.getAbsolutePath() + "/" + TABLE);
    sql("CREATE TABLE IF NOT EXISTS arcticCatalog." + db + "." + TABLE + "(" +
        " id INT, name STRING) WITH %s", toWithClause(tableProperties));

    sql("insert into arcticCatalog." + db + "." + TABLE + " /*+ OPTIONS(" +
        "'arctic.emit.mode'='file, log'" +
        ", 'log.version'='v1'" +
        ") */" +
        "select id, name from input");

    Assert.assertEquals(
        DataUtil.toRowSet(data), sqlSet("select * from arcticCatalog." + db + "." + TABLE +
            " /*+ OPTIONS('arctic.read.mode'='file', 'streaming'='false') */"));

    TableResult result = exec("select * from arcticCatalog." + db + "." + TABLE +
        " /*+ OPTIONS('arctic.read.mode'='log', 'scan.startup.mode'='earliest') */");
    Set<Row> actual = new HashSet<>();
    try (CloseableIterator<Row> iterator = result.collect()) {
      for (Object[] datum : data) {
        actual.add(iterator.next());
      }
    }
    Assert.assertEquals(DataUtil.toRowSet(data), actual);
    result.getJobClient().ifPresent(TestUtil::cancelJob);
  }

  @Test
  public void testPartitionSinkBatchRead() throws IOException {
    List<Object[]> data = new LinkedList<>();
    data.add(new Object[]{1000004, "a", "2022-05-17"});
    data.add(new Object[]{1000015, "b", "2022-05-17"});
    data.add(new Object[]{1000011, "c", "2022-05-17"});
    data.add(new Object[]{1000014, "d", "2022-05-18"});
    data.add(new Object[]{1000021, "d", "2022-05-18"});
    data.add(new Object[]{1000007, "e", "2022-05-18"});

    List<Object[]> expected = new LinkedList<>();
    expected.add(new Object[]{1000014, "d", "2022-05-18"});
    expected.add(new Object[]{1000021, "d", "2022-05-18"});
    expected.add(new Object[]{1000007, "e", "2022-05-18"});

    List<ApiExpression> rows = DataUtil.toRows(data);

    Table input = getTableEnv().fromValues(DataTypes.ROW(
            DataTypes.FIELD("id", DataTypes.INT()),
            DataTypes.FIELD("name", DataTypes.STRING()),
            DataTypes.FIELD("dt", DataTypes.STRING())
        ),
        rows
    );
    getTableEnv().createTemporaryView("input", input);

    sql("CREATE CATALOG arcticCatalog WITH %s", toWithClause(props));

    sql("CREATE TABLE IF NOT EXISTS arcticCatalog." + db + "." + TABLE + "(" +
        " id INT, name STRING, dt STRING)" +
        " PARTITIONED BY (dt)" +
        " WITH (" +
        " 'location' = '" + tableDir.getAbsolutePath() + "/" + TABLE + "'" +
        ")");

    sql("insert into arcticCatalog." + db + "." + TABLE +
        " PARTITION (dt='2022-05-18') select id, name from input" +
        " where dt='2022-05-18' ");

    TableIdentifier identifier = TableIdentifier.of(catalog, db, TABLE);
    ArcticTable table = arcticCatalog.loadTable(identifier);
    Iterable<Snapshot> snapshots = table.asUnkeyedTable().snapshots();
    Snapshot s = snapshots.iterator().next();

    Assert.assertEquals(DataUtil.toRowSet(expected), sqlSet("select * from arcticCatalog." + db + "." + TestUnkeyed.TABLE +
        "/*+ OPTIONS(" +
        "'arctic.read.mode'='file'" +
        ", 'snapshot-id'='" + s.snapshotId() + "'" +
        ", 'streaming'='false'" +
        ")*/" +
        ""));
    Assert.assertEquals(DataUtil.toRowSet(expected), sqlSet("select * from arcticCatalog." + db + "." + TestUnkeyed.TABLE +
        "/*+ OPTIONS(" +
        "'arctic.read.mode'='file'" +
        ", 'as-of-timestamp'='" + s.timestampMillis() + "'" +
        ", 'streaming'='false'" +
        ")*/" +
        ""));
  }

  @Test
  public void testPartitionSinkStreamRead() throws Exception {
    List<Object[]> data = new LinkedList<>();
    data.add(new Object[]{1000004, "a", "2022-05-17"});
    data.add(new Object[]{1000015, "b", "2022-05-17"});
    data.add(new Object[]{1000011, "c", "2022-05-17"});
    data.add(new Object[]{1000014, "d", "2022-05-18"});
    data.add(new Object[]{1000021, "d", "2022-05-18"});
    data.add(new Object[]{1000007, "e", "2022-05-18"});

    List<ApiExpression> rows = DataUtil.toRows(data);

    Table input = getTableEnv().fromValues(DataTypes.ROW(
            DataTypes.FIELD("id", DataTypes.INT()),
            DataTypes.FIELD("name", DataTypes.STRING()),
            DataTypes.FIELD("dt", DataTypes.STRING())
        ),
        rows
    );
    getTableEnv().createTemporaryView("input", input);

    sql("CREATE CATALOG arcticCatalog WITH %s", toWithClause(props));

    sql("CREATE TABLE IF NOT EXISTS arcticCatalog." + db + "." + TABLE + "(" +
        " id INT, name STRING, dt STRING)" +
        " PARTITIONED BY (dt)" +
        " WITH (" +
        " 'location' = '" + tableDir.getAbsolutePath() + "/" + TABLE + "'" +
        ")");

    sql("insert into arcticCatalog." + db + "." + TABLE +
        " PARTITION (dt='2022-05-18') select id, name from input" +
        " where dt='2022-05-18' ");
    sql("insert into arcticCatalog." + db + "." + TABLE +
        " PARTITION (dt='2022-05-18') select id, name from input" +
        " where dt='2022-05-18' ");

    TableIdentifier identifier = TableIdentifier.of(catalog, db, TABLE);
    ArcticTable table = arcticCatalog.loadTable(identifier);
    Iterable<Snapshot> snapshots = table.asUnkeyedTable().snapshots();
    Snapshot s = snapshots.iterator().next();

    TableResult result = exec("select * from arcticCatalog." + db + "." + TestUnkeyed.TABLE +
        "/*+ OPTIONS(" +
        "'arctic.read.mode'='file'" +
        ", 'start-snapshot-id'='" + s.snapshotId() + "'" +
        ")*/" +
        "");

    List<Row> expected = new ArrayList<Row>() {{
      add(Row.of(1000014, "d", "2022-05-18"));
      add(Row.of(1000021, "d", "2022-05-18"));
      add(Row.of(1000007, "e", "2022-05-18"));
    }};

    Set<Row> actual = new HashSet<>();
    try (CloseableIterator<Row> iterator = result.collect()) {
      for (int i = 0; i < expected.size(); i++) {
        actual.add(iterator.next());
      }
    }
    result.getJobClient().ifPresent(TestUtil::cancelJob);
    Assert.assertEquals(new HashSet<>(expected), actual);
  }

  @Test
  public void testPartitionLogSinkSource() throws Exception {
    List<Object[]> data = new LinkedList<>();
    data.add(new Object[]{1000004, "a", "2022-05-17"});
    data.add(new Object[]{1000015, "b", "2022-05-17"});
    data.add(new Object[]{1000011, "c", "2022-05-17"});
    data.add(new Object[]{1000014, "d", "2022-05-18"});
    data.add(new Object[]{1000021, "d", "2022-05-18"});
    data.add(new Object[]{1000007, "e", "2022-05-18"});

    List<ApiExpression> rows = DataUtil.toRows(data);

    Table input = getTableEnv().fromValues(DataTypes.ROW(
            DataTypes.FIELD("id", DataTypes.INT()),
            DataTypes.FIELD("name", DataTypes.STRING()),
            DataTypes.FIELD("dt", DataTypes.STRING())
        ),
        rows
    );
    getTableEnv().createTemporaryView("input", input);

    sql("CREATE CATALOG arcticCatalog WITH %s", toWithClause(props));

    tableProperties.put(LOCATION, tableDir.getAbsolutePath() + "/" + TABLE);
    sql("CREATE TABLE IF NOT EXISTS arcticCatalog." + db + "." + TABLE + "(" +
        " id INT, name STRING, dt STRING) PARTITIONED BY (dt) WITH %s", toWithClause(tableProperties));

    sql("insert into arcticCatalog." + db + "." + TABLE + " /*+ OPTIONS(" +
        "'arctic.emit.mode'='log'" +
        ", 'log.version'='v1'" +
        ") */" +
        " select * from input");

    TableResult result = exec("select * from arcticCatalog." + db + "." + TABLE +
        "/*+ OPTIONS(" +
        "'arctic.read.mode'='log'" +
        ", 'scan.startup.mode'='earliest'" +
        ")*/" +
        "");

    Set<Row> actual = new HashSet<>();
    try (CloseableIterator<Row> iterator = result.collect()) {
      for (Object[] datum : data) {
        actual.add(iterator.next());
      }
    }
    Assert.assertEquals(DataUtil.toRowSet(data), actual);

    result.getJobClient().ifPresent(TestUtil::cancelJob);
  }

  @Test
  public void testPartitionDoubleSink() throws Exception {
    List<Object[]> data = new LinkedList<>();
    data.add(new Object[]{1000004, "a", "2022-05-17"});
    data.add(new Object[]{1000015, "b", "2022-05-17"});
    data.add(new Object[]{1000011, "c", "2022-05-17"});
    data.add(new Object[]{1000014, "d", "2022-05-18"});
    data.add(new Object[]{1000021, "d", "2022-05-18"});
    data.add(new Object[]{1000007, "e", "2022-05-18"});

    List<ApiExpression> rows = DataUtil.toRows(data);

    Table input = getTableEnv().fromValues(DataTypes.ROW(
            DataTypes.FIELD("id", DataTypes.INT()),
            DataTypes.FIELD("name", DataTypes.STRING()),
            DataTypes.FIELD("dt", DataTypes.STRING())
        ),
        rows
    );
    getTableEnv().createTemporaryView("input", input);
    sql("CREATE CATALOG arcticCatalog WITH %s", toWithClause(props));

    tableProperties.put(LOCATION, tableDir.getAbsolutePath() + "/" + TABLE);
    sql("CREATE TABLE IF NOT EXISTS arcticCatalog." + db + "." + TABLE + "(" +
        " id INT, name STRING, dt STRING) PARTITIONED BY (dt) WITH %s", toWithClause(tableProperties));
    sql("insert into arcticCatalog." + db + "." + TABLE + " /*+ OPTIONS(" +
        "'arctic.emit.mode'='file, log'" +
        ", 'log.version'='v1'" +
        ") */" +
        "select * from input");

    Assert.assertEquals(DataUtil.toRowSet(data), sqlSet("select * from arcticCatalog." + db + "." + TABLE +
        " /*+ OPTIONS('arctic.read.mode'='file', 'streaming'='false') */"));
    TableResult result = exec("select * from arcticCatalog." + db + "." + TABLE +
        " /*+ OPTIONS('arctic.read.mode'='log', 'scan.startup.mode'='earliest') */");
    Set<Row> actual = new HashSet<>();
    try (CloseableIterator<Row> iterator = result.collect()) {
      for (Object[] datum : data) {
        actual.add(iterator.next());
      }
    }
    Assert.assertEquals(DataUtil.toRowSet(data), actual);
    result.getJobClient().ifPresent(TestUtil::cancelJob);
  }

}
