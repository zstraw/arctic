/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netease.arctic.hive.io;

import com.netease.arctic.hive.HiveTableTestBase;
import com.netease.arctic.hive.table.HiveLocationKind;
import java.io.IOException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.junit.Test;

import static com.netease.arctic.hive.io.TestIOUtils.testWrite;

public class TestAdaptHiveReader extends HiveTableTestBase {

  @Test
  public void testInt96WithoutTZPredicatePushDown() throws IOException {
    testWrite(testKeyedHiveTable, HiveLocationKind.INSTANT, HiveTestRecords.baseRecords(), "hive",
        Expressions.equal(COLUMN_NAME_OP_TIME, "2022-01-04T12:00:00"),
        ImmutableList.of(HiveTestRecords.baseRecords().get(1)));
  }

  @Test
  public void testInt96WithTZPredicatePushDown() throws IOException {
    testWrite(testKeyedHiveTable, HiveLocationKind.INSTANT, HiveTestRecords.baseRecords(), "hive",
        Expressions.equal(COLUMN_NAME_OP_TIME_WITH_ZONE, "2022-01-04T13:00:00+01:00"),
        ImmutableList.of(HiveTestRecords.baseRecords().get(1)));
  }

}
