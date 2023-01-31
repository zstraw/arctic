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

package com.netease.arctic.io.writer;

import com.netease.arctic.data.ChangeAction;
import com.netease.arctic.table.KeyedTable;
import com.netease.arctic.table.TableProperties;
import com.netease.arctic.utils.SchemaUtil;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.MetricsModes;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.PropertyUtil;

import java.util.Locale;

/**
 * Builder to create writers for {@link KeyedTable} writting {@link Record}.
 */
public class GenericTaskWriters {

  public static Builder builderFor(KeyedTable table) {
    return new Builder(table);
  }

  public static class Builder {

    private final KeyedTable table;

    private Long transactionId;
    private int partitionId = 0;
    private int taskId = 0;
    private ChangeAction changeAction = ChangeAction.INSERT;
    private boolean orderedWriter = false;

    Builder(KeyedTable table) {
      this.table = table;
    }

    public Builder withTransactionId(Long transactionId) {
      this.transactionId = transactionId;
      return this;
    }

    public Builder withPartitionId(int partitionId) {
      this.partitionId = partitionId;
      return this;
    }

    public Builder withTaskId(int taskId) {
      this.taskId = taskId;
      return this;
    }

    public Builder withChangeAction(ChangeAction changeAction) {
      this.changeAction = changeAction;
      return this;
    }

    public Builder withOrdered() {
      this.orderedWriter = true;
      return this;
    }

    public GenericBaseTaskWriter buildBaseWriter() {
      preconditions();
      FileFormat fileFormat = FileFormat.valueOf((table.properties().getOrDefault(TableProperties.BASE_FILE_FORMAT,
          TableProperties.BASE_FILE_FORMAT_DEFAULT).toUpperCase(Locale.ENGLISH)));
      long fileSizeBytes = PropertyUtil.propertyAsLong(table.properties(), TableProperties.WRITE_TARGET_FILE_SIZE_BYTES,
          TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);
      long mask = PropertyUtil.propertyAsLong(table.properties(), TableProperties.BASE_FILE_INDEX_HASH_BUCKET,
          TableProperties.BASE_FILE_INDEX_HASH_BUCKET_DEFAULT) - 1;
      return new GenericBaseTaskWriter(
          fileFormat,
          new GenericAppenderFactory(table.baseTable().schema(), table.spec()),
          new CommonOutputFileFactory(table.baseLocation(), table.spec(), fileFormat, table.io(),
              table.baseTable().encryption(), partitionId, taskId, transactionId),
          table.io(), fileSizeBytes, mask, table.baseTable().schema(),
          table.spec(), table.primaryKeySpec(), orderedWriter);
    }

    public SortedPosDeleteWriter<Record> buildBasePosDeleteWriter(long mask, long index, StructLike partitionKey) {
      preconditions();
      FileFormat fileFormat = FileFormat.valueOf((table.properties().getOrDefault(TableProperties.BASE_FILE_FORMAT,
          TableProperties.BASE_FILE_FORMAT_DEFAULT).toUpperCase(Locale.ENGLISH)));
      GenericAppenderFactory appenderFactory =
          new GenericAppenderFactory(table.baseTable().schema(), table.spec());
      appenderFactory.set(
          org.apache.iceberg.TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + MetadataColumns.DELETE_FILE_PATH.name(),
          MetricsModes.Full.get().toString());
      appenderFactory.set(
          org.apache.iceberg.TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + MetadataColumns.DELETE_FILE_POS.name(),
          MetricsModes.Full.get().toString());
      return new SortedPosDeleteWriter<>(appenderFactory,
          new CommonOutputFileFactory(table.baseLocation(), table.spec(), fileFormat, table.io(),
              table.baseTable().encryption(), partitionId, taskId, transactionId), table.io(),
          fileFormat, mask, index, partitionKey);
    }

    public GenericChangeTaskWriter buildChangeWriter() {
      preconditions();
      FileFormat fileFormat = FileFormat.valueOf((table.properties().getOrDefault(TableProperties.CHANGE_FILE_FORMAT,
          TableProperties.CHANGE_FILE_FORMAT_DEFAULT).toUpperCase(Locale.ENGLISH)));
      long fileSizeBytes = PropertyUtil.propertyAsLong(table.properties(), TableProperties.WRITE_TARGET_FILE_SIZE_BYTES,
          TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);
      long mask = PropertyUtil.propertyAsLong(table.properties(), TableProperties.CHANGE_FILE_INDEX_HASH_BUCKET,
          TableProperties.CHANGE_FILE_INDEX_HASH_BUCKET_DEFAULT) - 1;
      Schema changeWriteSchema = SchemaUtil.changeWriteSchema(table.changeTable().schema());
      return new GenericChangeTaskWriter(
          fileFormat,
          new GenericAppenderFactory(changeWriteSchema, table.spec()),
          new CommonOutputFileFactory(table.changeLocation(), table.spec(), fileFormat, table.io(),
              table.changeTable().encryption(), partitionId, taskId, transactionId),
          table.io(), fileSizeBytes, mask, table.changeTable().schema(), table.spec(), table.primaryKeySpec(),
          changeAction, orderedWriter);
    }

    private void preconditions() {
      if (table.isKeyedTable()) {
        Preconditions.checkNotNull(transactionId);
      } else {
        Preconditions.checkArgument(transactionId == null);
      }
    }
  }
}
