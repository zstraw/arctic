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

import com.netease.arctic.flink.extension.HMSExtension;
import com.netease.arctic.flink.read.AdaptHiveFlinkParquetReaders;
import com.netease.arctic.hive.table.HiveLocationKind;
import com.netease.arctic.table.ArcticTable;
import com.netease.arctic.table.BaseLocationKind;
import com.netease.arctic.table.ChangeLocationKind;
import com.netease.arctic.table.LocationKind;
import com.netease.arctic.table.WriteOperationKind;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.parquet.AdaptHiveParquet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterators;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.netease.arctic.flink.extension.HMSExtension.HIVE_TABLE_TEST_BASE;


@ExtendWith({HMSExtension.class})
public class AdaptHiveWriterTest {

  @Test
  public void testWriteTypeFromOperateKind() {
    {
      FlinkTaskWriterBuilder builder = FlinkTaskWriterBuilder
          .buildFor(HIVE_TABLE_TEST_BASE.testKeyedHiveTable)
          .withFlinkSchema(FlinkSchemaUtil.convert(HIVE_TABLE_TEST_BASE.testKeyedHiveTable.schema()))
          .withTransactionId(1L);

      Assert.assertTrue(builder.buildWriter(ChangeLocationKind.INSTANT) instanceof FlinkChangeTaskWriter);
      Assert.assertTrue(builder.buildWriter(BaseLocationKind.INSTANT) instanceof FlinkBaseTaskWriter);
      Assert.assertTrue(builder.buildWriter(HiveLocationKind.INSTANT) instanceof FlinkBaseTaskWriter);

      Assert.assertTrue(builder.buildWriter(WriteOperationKind.APPEND) instanceof FlinkChangeTaskWriter);
      Assert.assertTrue(builder.buildWriter(WriteOperationKind.OVERWRITE) instanceof FlinkBaseTaskWriter);
      Assert.assertTrue(builder.buildWriter(WriteOperationKind.MINOR_OPTIMIZE) instanceof FlinkBaseTaskWriter);
      Assert.assertTrue(builder.buildWriter(WriteOperationKind.MAJOR_OPTIMIZE) instanceof FlinkBaseTaskWriter);
      Assert.assertTrue(builder.buildWriter(WriteOperationKind.FULL_OPTIMIZE) instanceof FlinkBaseTaskWriter);
    }
    {
      FlinkTaskWriterBuilder builder = FlinkTaskWriterBuilder
          .buildFor(HIVE_TABLE_TEST_BASE.testHiveTable)
          .withFlinkSchema(FlinkSchemaUtil.convert(HIVE_TABLE_TEST_BASE.testHiveTable.schema()));

      Assert.assertTrue(builder.buildWriter(BaseLocationKind.INSTANT) instanceof FlinkBaseTaskWriter);
      Assert.assertTrue(builder.buildWriter(HiveLocationKind.INSTANT) instanceof FlinkBaseTaskWriter);

      Assert.assertTrue(builder.buildWriter(WriteOperationKind.APPEND) instanceof FlinkBaseTaskWriter);
      Assert.assertTrue(builder.buildWriter(WriteOperationKind.OVERWRITE) instanceof FlinkBaseTaskWriter);
      Assert.assertTrue(builder.buildWriter(WriteOperationKind.MAJOR_OPTIMIZE) instanceof FlinkBaseTaskWriter);
      Assert.assertTrue(builder.buildWriter(WriteOperationKind.FULL_OPTIMIZE) instanceof FlinkBaseTaskWriter);
    }
  }

  @Test
  public void testKeyedTableChangeWriteByLocationKind() throws IOException {
    testWrite(HIVE_TABLE_TEST_BASE.testKeyedHiveTable, ChangeLocationKind.INSTANT, geneRowData(), "change");
  }

  @Test
  public void testKeyedTableBaseWriteByLocationKind() throws IOException {
    testWrite(HIVE_TABLE_TEST_BASE.testKeyedHiveTable, BaseLocationKind.INSTANT, geneRowData(), "base");
  }

  @Test
  public void testKeyedTableHiveWriteByLocationKind() throws IOException {
    testWrite(HIVE_TABLE_TEST_BASE.testKeyedHiveTable, HiveLocationKind.INSTANT, geneRowData(), "hive");
  }

  @Test
  public void testUnPartitionKeyedTableChangeWriteByLocationKind() throws IOException {
    testWrite(HIVE_TABLE_TEST_BASE.testUnPartitionKeyedHiveTable, ChangeLocationKind.INSTANT, geneRowData(), "change");
  }

  @Test
  public void testUnPartitionKeyedTableBaseWriteByLocationKind() throws IOException {
    testWrite(HIVE_TABLE_TEST_BASE.testUnPartitionKeyedHiveTable, BaseLocationKind.INSTANT, geneRowData(), "base");
  }

  @Test
  public void testUnPartitionKeyedTableHiveWriteByLocationKind() throws IOException {
    testWrite(HIVE_TABLE_TEST_BASE.testUnPartitionKeyedHiveTable, HiveLocationKind.INSTANT, geneRowData(), "hive");
  }

  @Test
  public void testUnKeyedTableChangeWriteByLocationKind() throws IOException {
    try {
      testWrite(HIVE_TABLE_TEST_BASE.testHiveTable, ChangeLocationKind.INSTANT, geneRowData(), "change");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof IllegalArgumentException);
    }
  }

  @Test
  public void testUnKeyedTableBaseWriteByLocationKind() throws IOException {
    testWrite(HIVE_TABLE_TEST_BASE.testHiveTable, BaseLocationKind.INSTANT, geneRowData(), "base");
  }

  @Test
  public void testUnKeyedTableHiveWriteByLocationKind() throws IOException {
    testWrite(HIVE_TABLE_TEST_BASE.testHiveTable, HiveLocationKind.INSTANT, geneRowData(), "hive");
  }

  @Test
  public void testUnPartitionUnKeyedTableChangeWriteByLocationKind() throws IOException {
    try {
      testWrite(HIVE_TABLE_TEST_BASE.testUnPartitionHiveTable, ChangeLocationKind.INSTANT, geneRowData(), "change");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof IllegalArgumentException);
    }
  }

  @Test
  public void testUnPartitionUnKeyedTableBaseWriteByLocationKind() throws IOException {
    testWrite(HIVE_TABLE_TEST_BASE.testUnPartitionHiveTable, BaseLocationKind.INSTANT, geneRowData(), "base");
  }

  @Test
  public void testUnPartitionUnKeyedTableHiveWriteByLocationKind() throws IOException {
    testWrite(HIVE_TABLE_TEST_BASE.testUnPartitionHiveTable, HiveLocationKind.INSTANT, geneRowData(), "hive");
  }

  public void testWrite(ArcticTable table, LocationKind locationKind, List<RowData> records, String pathFeature) throws IOException {
    FlinkTaskWriterBuilder builder = FlinkTaskWriterBuilder
        .buildFor(table)
        .withFlinkSchema(FlinkSchemaUtil.convert(table.schema()))
        .withTransactionId(table.isKeyedTable() ? 1L : null);

    TaskWriter<RowData> changeWrite = builder.buildWriter(locationKind);
    for (RowData record : records) {
      changeWrite.write(record);
    }
    WriteResult complete = changeWrite.complete();
    Arrays.stream(complete.dataFiles()).forEach(s -> Assert.assertTrue(s.path().toString().contains(pathFeature)));
    CloseableIterable<RowData> concat =
        CloseableIterable.concat(Arrays.stream(complete.dataFiles()).map(s -> readParquet(
            table.schema(),
            s.path().toString())).collect(Collectors.toList()));
    Set<RowData> result = new HashSet<>();
    Iterators.addAll(result, concat.iterator());
    Assert.assertEquals(result, records.stream().collect(Collectors.toSet()));
  }

  private CloseableIterable<RowData> readParquet(Schema schema, String path) {
    AdaptHiveParquet.ReadBuilder builder = AdaptHiveParquet.read(
            Files.localInput(path))
        .project(schema)
        .createReaderFunc(fileSchema -> AdaptHiveFlinkParquetReaders.buildReader(schema, fileSchema, new HashMap<>()))
        .caseSensitive(false);

    CloseableIterable<RowData> iterable = builder.build();
    return iterable;
  }

  private List<RowData> geneRowData() {
    RowData rowData = GenericRowData.of(
        1,
        TimestampData.fromLocalDateTime(LocalDateTime.of(2022, 1, 1, 10, 0, 0)),
        TimestampData.fromInstant(LocalDateTime.of(2022, 1, 1, 10, 0, 0).toInstant(ZoneOffset.ofHours(8))),
        DecimalData.fromBigDecimal(new BigDecimal("100"), 3, 0),
        StringData.fromString("jack")
    );
    return Lists.newArrayList(rowData);
  }
}
