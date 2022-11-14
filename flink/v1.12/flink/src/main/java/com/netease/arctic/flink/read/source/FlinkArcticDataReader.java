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

package com.netease.arctic.flink.read.source;

import com.netease.arctic.data.DataTreeNode;
import com.netease.arctic.flink.read.AdaptHiveFlinkParquetReaders;
import com.netease.arctic.hive.io.reader.AdaptHiveBaseIcebergDataReader;
import com.netease.arctic.io.ArcticFileIO;
import com.netease.arctic.scan.ArcticFileScanTask;
import com.netease.arctic.table.PrimaryKeySpec;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.RowDataWrapper;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.parquet.ParquetValueReader;
import org.apache.iceberg.types.Type;
import org.apache.parquet.schema.MessageType;

import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * This is an arctic reader accepts a {@link FileScanTask} and produces a {@link CloseableIterator<RowData>}.
 * The RowData read from this reader may have more columns than the original schema.
 * The additional columns are added after the original columns,
 * see {@link com.netease.arctic.iceberg.optimize.DeleteFilter}.
 * It shall be projected before sent to downstream. This can be processed in {@link DataIterator#next()}
 */
public class FlinkArcticDataReader extends AdaptHiveBaseIcebergDataReader<RowData>
    implements FileScanTaskReader<RowData> {
  private static final long serialVersionUID = -6773693031945244386L;

  public FlinkArcticDataReader(
      ArcticFileIO fileIO, Schema tableSchema, Schema projectedSchema, String nameMapping, boolean caseSensitive,
      BiFunction<Type, Object, Object> convertConstant, boolean reuseContainer) {
    super(fileIO, tableSchema, projectedSchema, nameMapping, caseSensitive, convertConstant, reuseContainer);
  }

  public FlinkArcticDataReader(
      ArcticFileIO fileIO, Schema tableSchema, Schema projectedSchema, PrimaryKeySpec primaryKeySpec,
      String nameMapping, boolean caseSensitive, BiFunction<Type, Object, Object> convertConstant,
      Set<DataTreeNode> sourceNodes, boolean reuseContainer) {
    super(fileIO, tableSchema, projectedSchema, primaryKeySpec, nameMapping, caseSensitive, convertConstant,
        sourceNodes, reuseContainer);
  }

  @Override
  protected Function<MessageType, ParquetValueReader<?>> getNewReaderFunction(
      Schema projectSchema, Map<Integer, ?> idToConstant) {
    return fileSchema -> AdaptHiveFlinkParquetReaders.buildReader(projectSchema, fileSchema, idToConstant);
  }

  @Override
  protected Function<Schema, Function<RowData, StructLike>> toStructLikeFunction() {
    return schema -> {
      RowType requiredRowType = FlinkSchemaUtil.convert(schema);
      RowDataWrapper asStructLike = new RowDataWrapper(requiredRowType, schema.asStruct());
      return asStructLike::wrap;
    };
  }

  @Override
  public CloseableIterator<RowData> open(FileScanTask fileScanTask) {
    ArcticFileScanTask arcticFileScanTask = (ArcticFileScanTask) fileScanTask;
    CloseableIterable<RowData> rowDataIterable = readData(arcticFileScanTask);
    return fileIO.doAs(rowDataIterable::iterator);
  }
}
