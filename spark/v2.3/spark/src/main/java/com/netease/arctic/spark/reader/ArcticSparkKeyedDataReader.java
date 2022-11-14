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

package com.netease.arctic.spark.reader;

import com.netease.arctic.hive.io.reader.AdaptHiveBaseArcticDataReader;
import com.netease.arctic.io.ArcticFileIO;
import com.netease.arctic.spark.parquet.SparkParquetRowReaders;
import com.netease.arctic.spark.util.ArcticSparkUtil;
import com.netease.arctic.table.PrimaryKeySpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.parquet.ParquetValueReader;
import org.apache.iceberg.spark.SparkStructLike;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.sql.Row;

import java.util.Map;
import java.util.function.Function;

public class ArcticSparkKeyedDataReader extends AdaptHiveBaseArcticDataReader<Row> {

  public ArcticSparkKeyedDataReader(
      ArcticFileIO fileIO,
      Schema tableSchema,
      Schema projectedSchema,
      PrimaryKeySpec primaryKeySpec,
      String nameMapping,
      boolean caseSensitive) {
    super(fileIO, tableSchema, projectedSchema, primaryKeySpec, nameMapping, caseSensitive,
        ArcticSparkUtil::convertConstant, false);
  }

  @Override
  protected Function<MessageType, ParquetValueReader<?>> getNewReaderFunction(
      Schema projectSchema,
      Map<Integer, ?> idToConstant) {
    return fileSchema -> SparkParquetRowReaders.buildReader(projectSchema, fileSchema, idToConstant);
  }

  @Override
  protected Function<Schema, Function<Row, StructLike>> toStructLikeFunction() {
    return schema -> {
      return row -> new SparkStructLike(schema.asStruct()).wrap(row);
    };
  }
}
