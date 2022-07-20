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

import com.netease.arctic.flink.read.hybrid.enumerator.ContinuousSplitPlannerImplTest;
import com.netease.arctic.table.KeyedTable;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.RowKind;
import org.apache.iceberg.io.TaskWriter;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class RowDataReaderFunctionTest extends ContinuousSplitPlannerImplTest {
  private static final Logger LOG = LoggerFactory.getLogger(RowDataReaderFunctionTest.class);

  protected void assertArrayEquals(RowData[] excepts, List<RowData> actual) {
    Assert.assertArrayEquals(excepts, sortRowDataCollection(actual));
  }

  protected RowData[] sortRowDataCollection(Collection<RowData> records) {
    return records.stream().sorted(
            Comparator
                .comparing(
                    RowData::toString))
        .collect(Collectors.toList())
        .toArray(new RowData[records.size()]);
  }

  protected void writeUpdate(KeyedTable arcticTable) throws IOException {
    //write change update
    writeUpdate(updateRecords(), arcticTable);
  }

  protected void writeUpdate(List<RowData> input, KeyedTable arcticTable) throws IOException {
    //write change update
    {
      TaskWriter<RowData> taskWriter = createTaskWriter(false, arcticTable);

      for (RowData record : input) {
        taskWriter.write(record);
      }
      commit(taskWriter.complete(), false, arcticTable);
    }
  }

  protected List<RowData> updateRecords() {
    List<RowData> excepts = new ArrayList<>();
    excepts.add(GenericRowData.ofKind(RowKind.UPDATE_BEFORE, 5, StringData.fromString("lind"), TimestampData.fromLocalDateTime(ldt)));
    excepts.add(GenericRowData.ofKind(RowKind.UPDATE_AFTER, 5, StringData.fromString("lina"), TimestampData.fromLocalDateTime(ldt)));
    return excepts;
  }

  protected RowData[] excepts2() {
    List<RowData> excepts = updateRecords();

    return updateRecords().stream().sorted(Comparator.comparing(RowData::toString))
        .collect(Collectors.toList())
        .toArray(new RowData[excepts.size()]);
  }

  protected RowData[] excepts() {
    List<RowData> excepts = exceptsCollection();

    return excepts.stream().sorted(Comparator.comparing(RowData::toString))
        .collect(Collectors.toList())
        .toArray(new RowData[excepts.size()]);
  }

  protected List<RowData> exceptsCollection() {
    List<RowData> excepts = new ArrayList<>();
    excepts.add(GenericRowData.ofKind(RowKind.INSERT, 1, StringData.fromString("john"), TimestampData.fromLocalDateTime(ldt)));
    excepts.add(GenericRowData.ofKind(RowKind.INSERT, 2, StringData.fromString("lily"), TimestampData.fromLocalDateTime(ldt)));
    excepts.add(GenericRowData.ofKind(RowKind.INSERT, 3, StringData.fromString("jake"), TimestampData.fromLocalDateTime(ldt.plusDays(1))));
    excepts.add(GenericRowData.ofKind(RowKind.INSERT, 4, StringData.fromString("sam"), TimestampData.fromLocalDateTime(ldt.plusDays(1))));
    excepts.add(GenericRowData.ofKind(RowKind.INSERT, 5, StringData.fromString("mary"), TimestampData.fromLocalDateTime(ldt)));
    excepts.add(GenericRowData.ofKind(RowKind.INSERT, 6, StringData.fromString("mack"), TimestampData.fromLocalDateTime(ldt)));
    excepts.add(GenericRowData.ofKind(RowKind.DELETE, 5, StringData.fromString("mary"), TimestampData.fromLocalDateTime(ldt)));
    excepts.add(GenericRowData.ofKind(RowKind.INSERT, 5, StringData.fromString("lind"), TimestampData.fromLocalDateTime(ldt)));
    return excepts;
  }

}