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

package com.netease.arctic.spark.writer;

import com.netease.arctic.table.ArcticTable;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.spark.SparkFilters;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.SupportsDynamicOverwrite;
import org.apache.spark.sql.connector.write.SupportsOverwrite;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class ArcticSparkWriteBuilder implements WriteBuilder, SupportsDynamicOverwrite, SupportsOverwrite {

  public interface ArcticWrite {

    BatchWrite asBatchAppend();

    BatchWrite asDynamicOverwrite();

    BatchWrite asOverwriteByFilter(Expression overwriteExpr);

    BatchWrite asUpsertWrite();

    BatchWrite asMergeBatchWrite();
  }

  protected final CaseInsensitiveStringMap options;

  protected Expression overwriteExpr = null;

  private WriteMode writeMode = WriteMode.APPEND;
  private final ArcticWrite write;

  public ArcticSparkWriteBuilder(ArcticTable table, LogicalWriteInfo info) {
    this.options = info.options();
    if (options.containsKey(WriteMode.WRITE_MODE_KEY)) {
      this.writeMode = WriteMode.getWriteMode(options.get(WriteMode.WRITE_MODE_KEY));
    }

    if (table.isKeyedTable()) {
      write = new KeyedSparkBatchWrite(table.asKeyedTable(), info);
    } else {
      write = new UnkeyedSparkBatchWrite(table.asUnkeyedTable(), info);
    }
  }

  @Override
  public WriteBuilder overwriteDynamicPartitions() {
    Preconditions.checkState(overwriteExpr == null, "Cannot overwrite dynamically and by filter: %s", overwriteExpr);
    writeMode = WriteMode.OVERWRITE_DYNAMIC;
    return this;
  }

  @Override
  public WriteBuilder overwrite(Filter[] filters) {
    this.overwriteExpr = SparkFilters.convert(filters);
    String overwriteMode = options.getOrDefault("overwrite-mode", "null");
    if (overwriteExpr == Expressions.alwaysTrue() && "dynamic".equals(overwriteMode)) {
      writeMode = WriteMode.OVERWRITE_DYNAMIC;
    } else {
      writeMode = WriteMode.OVERWRITE_BY_FILTER;
    }
    return this;
  }

  @Override
  public BatchWrite buildForBatch() {
    switch (writeMode) {
      case APPEND:
        return write.asBatchAppend();
      case OVERWRITE_BY_FILTER:
        return write.asOverwriteByFilter(overwriteExpr);
      case OVERWRITE_DYNAMIC:
        return write.asDynamicOverwrite();
      case UPSERT:
        return write.asUpsertWrite();
      case MERGE:
        return write.asMergeBatchWrite();
      default:
        throw new UnsupportedOperationException("unsupported write mode: " + writeMode);
    }
  }
}
