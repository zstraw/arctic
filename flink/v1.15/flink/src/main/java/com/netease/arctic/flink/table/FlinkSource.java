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

import com.netease.arctic.flink.interceptor.ProxyFactory;
import com.netease.arctic.flink.read.ArcticSource;
import com.netease.arctic.flink.read.hybrid.reader.RowDataReaderFunction;
import com.netease.arctic.flink.read.source.ArcticScanContext;
import com.netease.arctic.flink.util.ArcticUtils;
import com.netease.arctic.flink.util.IcebergClassUtil;
import com.netease.arctic.flink.util.ProxyUtil;
import com.netease.arctic.table.ArcticTable;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.InputFormatSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.transformations.LegacySourceTransformation;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.source.FlinkInputFormat;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.PropertyUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.netease.arctic.flink.FlinkSchemaUtil.filterWatermark;
import static com.netease.arctic.flink.FlinkSchemaUtil.toRowType;
import static com.netease.arctic.flink.table.descriptors.ArcticValidator.DIM_TABLE_ENABLE;

/**
 * An util class create arctic source data stream.
 */
public class FlinkSource {
  private FlinkSource() {
  }

  public static Builder forRowData() {
    return new Builder();
  }

  public static final class Builder {

    private static final String ARCTIC_FILE_TRANSFORMATION = "arctic-file";
    private ProviderContext context;
    private StreamExecutionEnvironment env;
    private ArcticTable arcticTable;
    private ArcticTableLoader tableLoader;
    private TableSchema projectedSchema;
    private List<Expression> filters;
    private ReadableConfig flinkConf = new Configuration();
    private Map<String, String> properties = new HashMap<>();
    private long limit = -1L;
    private WatermarkStrategy<RowData> watermarkStrategy = WatermarkStrategy.noWatermarks();
    private final ArcticScanContext.Builder contextBuilder = ArcticScanContext.arcticBuilder();

    private Builder() {
    }

    public Builder context(ProviderContext context) {
      this.context = context;
      return this;
    }

    public Builder env(StreamExecutionEnvironment env) {
      this.env = env;
      return this;
    }

    public Builder arcticTable(ArcticTable arcticTable) {
      this.arcticTable = arcticTable;
      properties.putAll(arcticTable.properties());
      return this;
    }

    public Builder tableLoader(ArcticTableLoader tableLoader) {
      this.tableLoader = tableLoader;
      return this;
    }

    public Builder project(TableSchema tableSchema) {
      this.projectedSchema = tableSchema;
      return this;
    }

    public Builder limit(long limit) {
      this.limit = limit;
      contextBuilder.limit(limit);
      return this;
    }

    public Builder filters(List<Expression> filters) {
      this.filters = filters;
      contextBuilder.filters(filters);
      return this;
    }

    public Builder flinkConf(ReadableConfig flinkConf) {
      this.flinkConf = flinkConf;
      return this;
    }

    public Builder properties(Map<String, String> properties) {
      this.properties.putAll(properties);
      return this;
    }

    public Builder watermarkStrategy(WatermarkStrategy<RowData> watermarkStrategy) {
      if (watermarkStrategy != null) {
        this.watermarkStrategy = watermarkStrategy;
      }
      return this;
    }

    public DataStream<RowData> build() {
      Preconditions.checkNotNull(env, "StreamExecutionEnvironment should not be null");
      loadTableIfNeeded();

      if (arcticTable.isUnkeyedTable()) {
        return buildUnkeyedTableSource();
      }

      if (projectedSchema == null) {
        contextBuilder.project(arcticTable.schema());
      } else {
        contextBuilder.project(FlinkSchemaUtil.convert(arcticTable.schema(), filterWatermark(projectedSchema)));
      }
      contextBuilder.fromProperties(properties);
      ArcticScanContext scanContext = contextBuilder.build();

      RowDataReaderFunction rowDataReaderFunction = new RowDataReaderFunction(
          flinkConf,
          arcticTable.schema(),
          scanContext.project(),
          arcticTable.asKeyedTable().primaryKeySpec(),
          scanContext.nameMapping(),
          scanContext.caseSensitive(),
          arcticTable.io()
      );

      boolean dimTable = PropertyUtil.propertyAsBoolean(properties, DIM_TABLE_ENABLE.key(),
          DIM_TABLE_ENABLE.defaultValue());
      RowType rowType;
      if (projectedSchema != null) {
        // If dim table is enabled, we reserve a RowTime field in Emitter.
        if (dimTable) {
          rowType = toRowType(projectedSchema);
        } else {
          rowType = toRowType(filterWatermark(projectedSchema));
        }
      } else {
        rowType = FlinkSchemaUtil.convert(scanContext.project());
      }

      DataStreamSource<RowData> sourceStream = env.fromSource(
          new ArcticSource<>(tableLoader, scanContext, rowDataReaderFunction,
              InternalTypeInfo.of(rowType), arcticTable.name(), dimTable),
          watermarkStrategy, ArcticSource.class.getName());
      context.generateUid(ARCTIC_FILE_TRANSFORMATION).ifPresent(sourceStream::uid);
      return sourceStream;
    }

    private void loadTableIfNeeded() {
      if (tableLoader == null || arcticTable != null) {
        return;
      }
      arcticTable = ArcticUtils.loadArcticTable(tableLoader);
      properties.putAll(arcticTable.properties());
    }

    public DataStream<RowData> buildUnkeyedTableSource() {
      DataStream<RowData> origin = org.apache.iceberg.flink.source.FlinkSource.forRowData()
          .env(env)
          .project(projectedSchema)
          .tableLoader(tableLoader)
          .filters(filters)
          .properties(properties)
          .flinkConf(flinkConf)
          .limit(limit)
          .build();
      return wrapKrb(origin);
    }

    /**
     * extract op from dataStream, and wrap krb support
     */
    private DataStream<RowData> wrapKrb(DataStream<RowData> ds) {
      IcebergClassUtil.clean(env);
      Transformation origin = ds.getTransformation();

      if (origin instanceof OneInputTransformation) {
        OneInputTransformation<RowData, RowData> tf = (OneInputTransformation<RowData, RowData>) ds.getTransformation();
        OneInputStreamOperatorFactory op = (OneInputStreamOperatorFactory) tf.getOperatorFactory();
        ProxyFactory<FlinkInputFormat> inputFormatProxyFactory
            = IcebergClassUtil.getInputFormatProxyFactory(op, arcticTable.io(), arcticTable.schema());

        if (tf.getInputs().isEmpty()) {
          return env.addSource(new UnkeyedInputFormatSourceFunction(inputFormatProxyFactory, tf.getOutputType()))
              .setParallelism(tf.getParallelism());
        }

        LegacySourceTransformation tfSource = (LegacySourceTransformation) tf.getInputs().get(0);
        StreamSource source = tfSource.getOperator();
        SourceFunction function = IcebergClassUtil.getSourceFunction(source);

        SourceFunction functionProxy = (SourceFunction) ProxyUtil.getProxy(function, arcticTable.io());
        DataStreamSource sourceStream = env.addSource(functionProxy, tfSource.getName(),
            tfSource.getOutputType());
        context.generateUid(ARCTIC_FILE_TRANSFORMATION).ifPresent(sourceStream::uid);
        return sourceStream.transform(
            tf.getName(),
            tf.getOutputType(),
            new UnkeyedInputFormatOperatorFactory(inputFormatProxyFactory));
      }

      LegacySourceTransformation tfSource = (LegacySourceTransformation) origin;
      StreamSource source = tfSource.getOperator();
      InputFormatSourceFunction function = (InputFormatSourceFunction) IcebergClassUtil.getSourceFunction(source);

      InputFormat inputFormatProxy = (InputFormat) ProxyUtil.getProxy(function.getFormat(), arcticTable.io());
      DataStreamSource sourceStream =
          env.createInput(inputFormatProxy, tfSource.getOutputType()).setParallelism(origin.getParallelism());
      context.generateUid(ARCTIC_FILE_TRANSFORMATION).ifPresent(sourceStream::uid);
      return sourceStream;
    }
  }
}