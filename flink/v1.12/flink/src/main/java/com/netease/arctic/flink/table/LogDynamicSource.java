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

import com.netease.arctic.flink.read.FlinkKafkaConsumer;
import com.netease.arctic.flink.read.LogKafkaConsumer;
import com.netease.arctic.flink.read.source.log.kafka.LogKafkaSource;
import com.netease.arctic.flink.read.source.log.kafka.LogKafkaSourceBuilder;
import com.netease.arctic.flink.read.source.log.pulsar.LogPulsarSource;
import com.netease.arctic.flink.read.source.log.pulsar.LogPulsarSourceBuilder;
import com.netease.arctic.flink.table.descriptors.ArcticValidator;
import com.netease.arctic.flink.util.CompatibleFlinkPropertyUtil;
import com.netease.arctic.flink.util.Projection;
import com.netease.arctic.table.ArcticTable;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaDeserializationSchemaWrapper;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsWatermarkPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.netease.arctic.flink.table.descriptors.ArcticValidator.ARCTIC_LOG_CONSISTENCY_GUARANTEE_ENABLE;
import static com.netease.arctic.flink.table.descriptors.ArcticValidator.ARCTIC_LOG_CONSUMER_CHANGELOG_MODE;
import static com.netease.arctic.flink.table.descriptors.ArcticValidator.ARCTIC_LOG_KAFKA_COMPATIBLE_ENABLE;
import static com.netease.arctic.flink.table.descriptors.ArcticValidator.LOG_CONSUMER_CHANGELOG_MODE_ALL_KINDS;
import static com.netease.arctic.flink.table.descriptors.ArcticValidator.LOG_CONSUMER_CHANGELOG_MODE_APPEND_ONLY;
import static com.netease.arctic.flink.table.descriptors.ArcticValidator.SCAN_STARTUP_MODE_EARLIEST;
import static com.netease.arctic.flink.table.descriptors.ArcticValidator.SCAN_STARTUP_MODE_LATEST;
import static com.netease.arctic.flink.table.descriptors.ArcticValidator.SCAN_STARTUP_MODE_TIMESTAMP;
import static com.netease.arctic.table.TableProperties.LOG_STORE_STORAGE_TYPE_DEFAULT;
import static com.netease.arctic.table.TableProperties.LOG_STORE_STORAGE_TYPE_KAFKA;
import static com.netease.arctic.table.TableProperties.LOG_STORE_STORAGE_TYPE_PULSAR;
import static com.netease.arctic.table.TableProperties.LOG_STORE_TYPE;
import static org.apache.flink.table.connector.ChangelogMode.insertOnly;

/**
 * This is a log source table api, create log queue consumer e.g. {@link LogKafkaSource}
 */
public class LogDynamicSource implements ScanTableSource, SupportsWatermarkPushDown, SupportsProjectionPushDown {

  private static final Logger LOG = LoggerFactory.getLogger(LogDynamicSource.class);
  
  private final ArcticTable arcticTable;
  private final Schema schema;
  private final ReadableConfig tableOptions;
  private final Optional<String> consumerChangelogMode;
  private final boolean logRetractionEnable;

  /**
   * Watermark strategy that is used to generate per-partition watermark.
   */
  protected @Nullable WatermarkStrategy<RowData> watermarkStrategy;

  /** Data type to configure the formats. */

  /**
   * Indices that determine the value fields and the target position in the produced row.
   */
  protected final int[] valueProjection;
  /**
   * Field index paths of all fields that must be present in the physically produced data.
   */
  protected int[] projectedFields;

  /**
   * Data type to configure the formats.
   *
   * @deprecated since 0.4.1, will be removed in 0.7.0;
   */
  @Nullable
  @Deprecated
  protected DataType physicalDataType;

  /**
   * Format for decoding values from Kafka.
   *
   * @deprecated since 0.4.1, will be removed in 0.7.0;
   */
  @Nullable
  @Deprecated
  protected DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat;

  /**
   * The logStore message queue's topics
   */
  protected final List<String> topics;

  /**
   * The logStore topic pattern to consume.
   */
  protected final Pattern topicPattern;

  /**
   * Properties for the logStore consumer.
   */
  protected final Properties properties;

  /**
   * The startup mode for the contained consumer (default is {@link StartupMode#GROUP_OFFSETS}).
   */
  protected final StartupMode startupMode;

  /**
   * The start timestamp to locate partition offsets; only relevant when startup mode is {@link
   * StartupMode#TIMESTAMP}.
   */
  protected final long startupTimestampMillis;

  private static final ChangelogMode ALL_KINDS = ChangelogMode.newBuilder()
      .addContainedKind(RowKind.INSERT)
      .addContainedKind(RowKind.UPDATE_BEFORE)
      .addContainedKind(RowKind.UPDATE_AFTER)
      .addContainedKind(RowKind.DELETE)
      .build();

  public static StartupMode toInternal(String startupMode) {
    startupMode = startupMode.toLowerCase();
    switch (startupMode) {
      case SCAN_STARTUP_MODE_LATEST:
        return StartupMode.LATEST;
      case SCAN_STARTUP_MODE_EARLIEST:
        return StartupMode.EARLIEST;
      case SCAN_STARTUP_MODE_TIMESTAMP:
        return StartupMode.TIMESTAMP;
      default:
        throw new ValidationException(String.format(
            "%s only support '%s', '%s'. But input is '%s'", ArcticValidator.SCAN_STARTUP_MODE,
            SCAN_STARTUP_MODE_LATEST, SCAN_STARTUP_MODE_EARLIEST, startupMode));
    }
  }

  public LogDynamicSource(
      DataType physicalDataType,
      DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat,
      int[] valueProjection,
      @Nullable List<String> topics,
      @Nullable Pattern topicPattern,
      Properties properties,
      String startupMode,
      long startupTimestampMillis,
      Schema schema,
      ReadableConfig tableOptions,
      ArcticTable arcticTable) {
    this.schema = schema;
    this.tableOptions = tableOptions;
    this.consumerChangelogMode = tableOptions.getOptional(ARCTIC_LOG_CONSUMER_CHANGELOG_MODE);
    this.logRetractionEnable = CompatibleFlinkPropertyUtil.propertyAsBoolean(arcticTable.properties(),
        ARCTIC_LOG_CONSISTENCY_GUARANTEE_ENABLE.key(), ARCTIC_LOG_CONSISTENCY_GUARANTEE_ENABLE.defaultValue());
    this.arcticTable = arcticTable;
    this.valueProjection = valueProjection;
    this.topics = topics;
    this.topicPattern = topicPattern;
    this.properties = properties;
    this.startupMode = toInternal(startupMode);
    this.startupTimestampMillis = startupTimestampMillis;
    this.physicalDataType = physicalDataType;
    this.valueDecodingFormat = valueDecodingFormat;
  }

  LogDynamicSource(
      DataType physicalDataType,
      DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat,
      int[] valueProjection,
      @Nullable List<String> topics,
      @Nullable Pattern topicPattern,
      Properties properties,
      StartupMode startupMode,
      long startupTimestampMillis,
      Schema schema,
      ReadableConfig tableOptions,
      ArcticTable arcticTable,
      boolean logRetractionEnable,
      Optional<String> consumerChangelogMode,
      int[] projectedFields) {
    this.schema = schema;
    this.tableOptions = tableOptions;
    this.consumerChangelogMode = consumerChangelogMode;
    this.logRetractionEnable = logRetractionEnable;
    this.arcticTable = arcticTable;
    this.valueProjection = valueProjection;
    this.topics = topics;
    this.topicPattern = topicPattern;
    this.properties = properties;
    this.startupMode = startupMode;
    this.startupTimestampMillis = startupTimestampMillis;
    this.projectedFields = projectedFields;
    this.physicalDataType = physicalDataType;
    this.valueDecodingFormat = valueDecodingFormat;
  }

  protected LogKafkaSource createKafkaSource() {
    Schema projectedSchema = schema;
    if (projectedFields != null) {
      final List<Types.NestedField> columns = schema.columns();
      projectedSchema = new Schema(Arrays.stream(projectedFields).mapToObj(columns::get).collect(Collectors.toList()));
    }

    LogKafkaSourceBuilder kafkaSourceBuilder = LogKafkaSource.builder(projectedSchema, arcticTable.properties());
    if (topics != null) {
      kafkaSourceBuilder.setTopics(topics);
    } else {
      kafkaSourceBuilder.setTopicPattern(topicPattern);
    }

    switch (startupMode) {
      case EARLIEST:
        kafkaSourceBuilder.setStartingOffsets(OffsetsInitializer.earliest());
        break;
      case LATEST:
        kafkaSourceBuilder.setStartingOffsets(OffsetsInitializer.latest());
        break;
      case GROUP_OFFSETS:
        kafkaSourceBuilder.setStartingOffsets(OffsetsInitializer.committedOffsets());
        break;
      case TIMESTAMP:
        kafkaSourceBuilder.setStartingOffsets(
            OffsetsInitializer.timestamp(startupTimestampMillis));
        break;
    }
    kafkaSourceBuilder.setProperties(properties);
    
    LOG.info("build log kafka source");
    return kafkaSourceBuilder.build();
  }

  protected LogPulsarSource createPulsarSource() {
    Schema projectedSchema = schema;
    if (valueProjection != null) {
      final List<Types.NestedField> columns = schema.columns();
      projectedSchema = new Schema(Arrays.stream(valueProjection).mapToObj(columns::get).collect(Collectors.toList()));
    }

    LogPulsarSourceBuilder pulsarSourceBuilder = LogPulsarSource.builder(projectedSchema, arcticTable.properties());
    if (topics != null) {
      pulsarSourceBuilder.setTopics(topics);
    } else {
      pulsarSourceBuilder.setTopicPattern(topicPattern);
    }

    switch (startupMode) {
      case EARLIEST:
        pulsarSourceBuilder.setStartCursor(StartCursor.earliest());
        break;
      case LATEST:
        pulsarSourceBuilder.setStartCursor(StartCursor.latest());
        break;
      case TIMESTAMP:
        pulsarSourceBuilder.setStartCursor(StartCursor.fromPublishTime(startupTimestampMillis));
        break;
    }
    pulsarSourceBuilder.setProperties(properties);

    LOG.info("build log pulsar source");
    return pulsarSourceBuilder.build();
  }

  /**
   * Use {@link #createKafkaSource()} instead.
   *
   * @deprecated since 0.4.1, will be removed in 0.7.0;
   */
  @Deprecated
  protected FlinkKafkaConsumer<RowData> createKafkaConsumer(DeserializationSchema<RowData> valueDeserialization) {
    final FlinkKafkaConsumer<RowData> kafkaConsumer;

    final KafkaDeserializationSchemaWrapper<RowData> deserializationSchemaWrapper =
        new KafkaDeserializationSchemaWrapper<>(valueDeserialization);
    Schema projectedSchema = schema;
    if (projectedFields != null) {
      final List<Types.NestedField> columns = schema.columns();
      projectedSchema = new Schema(Arrays.stream(projectedFields).mapToObj(columns::get).collect(Collectors.toList()));
    }
    if (topics != null) {
      kafkaConsumer =
          new LogKafkaConsumer(
              topics,
              deserializationSchemaWrapper,
              properties,
              projectedSchema,
              tableOptions);
    } else {
      kafkaConsumer =
          new LogKafkaConsumer(
              topicPattern,
              deserializationSchemaWrapper,
              properties,
              projectedSchema,
              tableOptions);
    }

    switch (startupMode) {
      case EARLIEST:
        kafkaConsumer.setStartFromEarliest();
        break;
      case LATEST:
        kafkaConsumer.setStartFromLatest();
        break;
      case GROUP_OFFSETS:
        kafkaConsumer.setStartFromGroupOffsets();
        break;
      case TIMESTAMP:
        kafkaConsumer.setStartFromTimestamp(startupTimestampMillis);
        break;
    }

    kafkaConsumer.setCommitOffsetsOnCheckpoints(properties.getProperty("group.id") != null);

    if (watermarkStrategy != null) {
      kafkaConsumer.assignTimestampsAndWatermarks(watermarkStrategy);
    }
    return kafkaConsumer;
  }

  /**
   * Use {@link #createKafkaSource()} instead.
   *
   * @deprecated since 0.4.1, will be removed in 0.7.0;
   */
  @Deprecated
  private FlinkKafkaConsumer<RowData> createKafkaConsumer(ScanContext scanContext) {
    if (useOldAPI()) {
      final DeserializationSchema<RowData> valueDeserialization =
          createDeserialization(scanContext, valueDecodingFormat, valueProjection, null);
      return createKafkaConsumer(valueDeserialization);
    }
    return null;
  }
  
  @Override
  public ChangelogMode getChangelogMode() {
    String changeLogMode = consumerChangelogMode.orElse(
        arcticTable.isKeyedTable() ? LOG_CONSUMER_CHANGELOG_MODE_ALL_KINDS : LOG_CONSUMER_CHANGELOG_MODE_APPEND_ONLY);
    switch (changeLogMode) {
      case LOG_CONSUMER_CHANGELOG_MODE_APPEND_ONLY:
        if (logRetractionEnable) {
          throw new IllegalArgumentException(
              String.format(
                  "Only %s is false when %s is %s",
                  ARCTIC_LOG_CONSISTENCY_GUARANTEE_ENABLE.key(),
                  ARCTIC_LOG_CONSUMER_CHANGELOG_MODE.key(),
                  LOG_CONSUMER_CHANGELOG_MODE_APPEND_ONLY));
        }
        return insertOnly();
      case LOG_CONSUMER_CHANGELOG_MODE_ALL_KINDS:
        return ALL_KINDS;
      default:
        throw new UnsupportedOperationException(
            String.format(
                "As of now, %s can't support this option %s.",
                ARCTIC_LOG_CONSUMER_CHANGELOG_MODE.key(),
                consumerChangelogMode
            ));
    }
  }

  private Source<RowData, ?, ?> buildLogSource() {
    String logType = CompatibleFlinkPropertyUtil.propertyAsString(arcticTable.properties(),
        LOG_STORE_TYPE, LOG_STORE_STORAGE_TYPE_DEFAULT).toLowerCase();
    switch (logType) {
      case LOG_STORE_STORAGE_TYPE_KAFKA:
        return createKafkaSource();
      case LOG_STORE_STORAGE_TYPE_PULSAR:
        return createPulsarSource();
      default:
        throw new UnsupportedOperationException("only support 'kafka' or 'pulsar' now, but input is " + logType);
    }
  }

  @Override
  public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
    final FlinkKafkaConsumer<RowData> kafkaConsumer = createKafkaConsumer(scanContext);
    final Source<RowData, ?, ?> logSource = buildLogSource();

    return new DataStreamScanProvider() {
      @Override
      public DataStream<RowData> produceDataStream(StreamExecutionEnvironment execEnv) {
        if (watermarkStrategy == null) {
          watermarkStrategy = WatermarkStrategy.noWatermarks();
        }

        if (useOldAPI()) {
          final TypeInformation<RowData> producedTypeInfo =
              scanContext.createTypeInformation(Optional.ofNullable(projectedFields)
                  .map(Projection::of)
                  .map(p -> p.project(physicalDataType)).orElse(physicalDataType));
          LOG.info("build deprecated log kafka source");
          return execEnv.addSource(kafkaConsumer, arcticTable.name(), producedTypeInfo);
        }
        return execEnv.fromSource(
            logSource, watermarkStrategy, "LogStoreSource-" + arcticTable.name());
      }

      @Override
      public boolean isBounded() {
        return logSource.getBoundedness() == Boundedness.BOUNDED;
      }
    };
  }

  /**
   * Return true only if {@link ArcticValidator#ARCTIC_LOG_KAFKA_COMPATIBLE_ENABLE} is true and
   * {@link LOG_STORE_TYPE} is kafka.
   */
  private boolean useOldAPI() {
    boolean useOldAPI =  CompatibleFlinkPropertyUtil.propertyAsBoolean(
        arcticTable.properties(),
        ArcticValidator.ARCTIC_LOG_KAFKA_COMPATIBLE_ENABLE.key(),
        ArcticValidator.ARCTIC_LOG_KAFKA_COMPATIBLE_ENABLE.defaultValue());
    if (useOldAPI) {
      String logType = CompatibleFlinkPropertyUtil.propertyAsString(arcticTable.properties(),
          LOG_STORE_TYPE, LOG_STORE_STORAGE_TYPE_DEFAULT).toLowerCase();
      if (!Objects.equals(LOG_STORE_STORAGE_TYPE_KAFKA, logType)) {
        LOG.warn("{} option only take effect for {} = {}.",
            ARCTIC_LOG_KAFKA_COMPATIBLE_ENABLE.key(), LOG_STORE_TYPE, LOG_STORE_STORAGE_TYPE_KAFKA);
        return false;
      }
      return true;
    }
    return false;
  }

  @Nullable
  private DeserializationSchema<RowData> createDeserialization(
      Context context,
      @Nullable DecodingFormat<DeserializationSchema<RowData>> format,
      int[] projection,
      @Nullable String prefix) {
    if (format == null) {
      return null;
    }
    DataType physicalFormatDataType =
        DataTypeUtils.projectRow(this.physicalDataType, projection);
    if (prefix != null) {
      physicalFormatDataType = DataTypeUtils.stripRowPrefix(physicalFormatDataType, prefix);
    }
    return format.createRuntimeDecoder(context, physicalFormatDataType);
  }

  @Override
  public DynamicTableSource copy() {
    return new LogDynamicSource(
        this.physicalDataType,
        this.valueDecodingFormat,
        this.valueProjection,
        this.topics,
        this.topicPattern,
        this.properties,
        this.startupMode,
        this.startupTimestampMillis,
        this.schema,
        this.tableOptions,
        this.arcticTable,
        this.logRetractionEnable,
        this.consumerChangelogMode,
        this.projectedFields);
  }

  @Override
  public String asSummaryString() {
    return "Arctic Log: " + arcticTable.name();
  }

  @Override
  public void applyWatermark(WatermarkStrategy<RowData> watermarkStrategy) {
    this.watermarkStrategy = watermarkStrategy;
  }

  @Override
  public boolean supportsNestedProjection() {
    // TODO: support nested projection
    return false;
  }

  @Override
  public void applyProjection(int[][] projectFields) {
    this.projectedFields = new int[projectFields.length];
    for (int i = 0; i < projectFields.length; i++) {
      Preconditions.checkArgument(
          projectFields[i].length == 1,
          "Don't support nested projection now.");
      this.projectedFields[i] = projectFields[i][0];
    }
  }
}
