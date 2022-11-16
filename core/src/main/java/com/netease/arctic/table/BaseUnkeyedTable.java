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

package com.netease.arctic.table;

import com.netease.arctic.AmsClient;
import com.netease.arctic.io.ArcticFileIO;
import com.netease.arctic.op.PartitionPropertiesUpdate;
import com.netease.arctic.op.UpdatePartitionProperties;
import com.netease.arctic.trace.AmsTableTracer;
import com.netease.arctic.trace.TableTracer;
import com.netease.arctic.trace.TracedAppendFiles;
import com.netease.arctic.trace.TracedDeleteFiles;
import com.netease.arctic.trace.TracedOverwriteFiles;
import com.netease.arctic.trace.TracedReplacePartitions;
import com.netease.arctic.trace.TracedRewriteFiles;
import com.netease.arctic.trace.TracedRowDelta;
import com.netease.arctic.trace.TracedSchemaUpdate;
import com.netease.arctic.trace.TracedTransaction;
import com.netease.arctic.trace.TracedUpdateProperties;
import com.netease.arctic.trace.TrackerOperations;
import com.netease.arctic.utils.TablePropertyUtil;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.ExpireSnapshots;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.HistoryEntry;
import org.apache.iceberg.ManageSnapshots;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.ReplacePartitions;
import org.apache.iceberg.ReplaceSortOrder;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.RewriteManifests;
import org.apache.iceberg.Rollback;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateLocation;
import org.apache.iceberg.UpdatePartitionSpec;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.util.StructLikeMap;
import org.apache.thrift.TException;

import java.util.List;
import java.util.Map;

/**
 * Base implementation of {@link UnkeyedTable}, wrapping a {@link Table}.
 */
public class BaseUnkeyedTable implements UnkeyedTable, HasTableOperations {

  private final TableIdentifier tableIdentifier;
  protected final Table icebergTable;
  protected final ArcticFileIO arcticFileIO;

  private final AmsClient client;

  public BaseUnkeyedTable(
      TableIdentifier tableIdentifier, Table icebergTable, ArcticFileIO arcticFileIO,
      AmsClient client) {
    this.tableIdentifier = tableIdentifier;
    this.icebergTable = icebergTable;
    this.arcticFileIO = arcticFileIO;
    this.client = client;
  }

  public BaseUnkeyedTable(TableIdentifier tableIdentifier, Table icebergTable, ArcticFileIO arcticFileIO) {
    this.tableIdentifier = tableIdentifier;
    this.icebergTable = icebergTable;
    this.arcticFileIO = arcticFileIO;
    this.client = null;
  }

  @Override
  public void refresh() {
    icebergTable.refresh();
  }

  @Override
  public TableScan newScan() {
    return icebergTable.newScan().includeColumnStats();
  }

  @Override
  public TableIdentifier id() {
    return tableIdentifier;
  }

  @Override
  public Schema schema() {
    return icebergTable.schema();
  }

  @Override
  public Map<Integer, Schema> schemas() {
    return icebergTable.schemas();
  }

  @Override
  public PartitionSpec spec() {
    return icebergTable.spec();
  }

  @Override
  public Map<Integer, PartitionSpec> specs() {
    return icebergTable.specs();
  }

  @Override
  public SortOrder sortOrder() {
    return icebergTable.sortOrder();
  }

  @Override
  public Map<Integer, SortOrder> sortOrders() {
    return icebergTable.sortOrders();
  }

  @Override
  public Map<String, String> properties() {
    return icebergTable.properties();
  }

  @Override
  public String location() {
    return icebergTable.location();
  }

  @Override
  public Snapshot currentSnapshot() {
    return icebergTable.currentSnapshot();
  }

  @Override
  public Snapshot snapshot(long snapshotId) {
    return icebergTable.snapshot(snapshotId);
  }

  @Override
  public Iterable<Snapshot> snapshots() {
    return icebergTable.snapshots();
  }

  @Override
  public List<HistoryEntry> history() {
    return icebergTable.history();
  }

  @Override
  public UpdateSchema updateSchema() {
    if (client != null) {
      return new TracedSchemaUpdate(icebergTable.updateSchema(), new AmsTableTracer(this, null, client));
    } else {
      return icebergTable.updateSchema();
    }
  }

  @Override
  public UpdatePartitionSpec updateSpec() {
    return icebergTable.updateSpec();
  }

  @Override
  public UpdateProperties updateProperties() {
    UpdateProperties updateProperties = icebergTable.updateProperties();
    if (client != null) {
      AmsTableTracer tracer = new AmsTableTracer(this, TrackerOperations.UPDATE_PROPERTIES, client);
      return new TracedUpdateProperties(updateProperties, tracer);
    } else {
      return updateProperties;
    }
  }

  @Override
  public ReplaceSortOrder replaceSortOrder() {
    return icebergTable.replaceSortOrder();
  }

  @Override
  public UpdateLocation updateLocation() {
    return icebergTable.updateLocation();
  }

  @Override
  public AppendFiles newAppend() {
    AppendFiles appendFiles = icebergTable.newAppend();
    if (client != null) {
      TableTracer tracer = new AmsTableTracer(this, TrackerOperations.APPEND, client);
      return new TracedAppendFiles(appendFiles, tracer);
    } else {
      return appendFiles;
    }
  }

  @Override
  public AppendFiles newFastAppend() {
    AppendFiles appendFiles = icebergTable.newFastAppend();
    if (client != null) {
      TableTracer tracer = new AmsTableTracer(this, TrackerOperations.APPEND, client);
      return new TracedAppendFiles(appendFiles, tracer);
    } else {
      return appendFiles;
    }
  }

  @Override
  public RewriteFiles newRewrite() {
    RewriteFiles rewriteFiles = icebergTable.newRewrite();
    if (client != null) {
      TableTracer tracer = new AmsTableTracer(this, TrackerOperations.REPLACE, client);
      return new TracedRewriteFiles(rewriteFiles, tracer);
    } else {
      return rewriteFiles;
    }
  }

  @Override
  public RewriteManifests rewriteManifests() {
    return icebergTable.rewriteManifests();
  }

  @Override
  public OverwriteFiles newOverwrite() {
    OverwriteFiles overwriteFiles = icebergTable.newOverwrite();
    if (client != null) {
      TableTracer tracer = new AmsTableTracer(this, TrackerOperations.OVERWRITE, client);
      return new TracedOverwriteFiles(overwriteFiles, tracer);
    } else {
      return overwriteFiles;
    }
  }

  @Override
  public RowDelta newRowDelta() {
    RowDelta rowDelta = icebergTable.newRowDelta();
    if (client != null) {
      TableTracer tracer = new AmsTableTracer(this, TrackerOperations.OVERWRITE, client);
      return new TracedRowDelta(rowDelta, tracer);
    } else {
      return rowDelta;
    }
  }

  @Override
  public ReplacePartitions newReplacePartitions() {
    ReplacePartitions replacePartitions = icebergTable.newReplacePartitions();
    if (client != null) {
      TableTracer tracer = new AmsTableTracer(this, TrackerOperations.OVERWRITE, client);
      return new TracedReplacePartitions(replacePartitions, tracer);
    } else {
      return replacePartitions;
    }
  }

  @Override
  public DeleteFiles newDelete() {
    DeleteFiles deleteFiles = icebergTable.newDelete();
    if (client != null) {
      TableTracer tracer = new AmsTableTracer(this, TrackerOperations.DELETE, client);
      return new TracedDeleteFiles(deleteFiles, tracer);
    } else {
      return deleteFiles;
    }
  }

  @Override
  public ExpireSnapshots expireSnapshots() {
    return icebergTable.expireSnapshots();
  }

  @Override
  public Rollback rollback() {
    return icebergTable.rollback();
  }

  @Override
  public ManageSnapshots manageSnapshots() {
    return icebergTable.manageSnapshots();
  }

  @Override
  public Transaction newTransaction() {
    Transaction transaction = icebergTable.newTransaction();
    if (client != null) {
      return new TracedTransaction(transaction, new AmsTableTracer(this, client));
    } else {
      return transaction;
    }
  }

  @Override
  public ArcticFileIO io() {
    return arcticFileIO;
  }

  @Override
  public EncryptionManager encryption() {
    return icebergTable.encryption();
  }

  @Override
  public LocationProvider locationProvider() {
    return icebergTable.locationProvider();
  }

  @Override
  public TableOperations operations() {
    if (icebergTable instanceof HasTableOperations) {
      return ((HasTableOperations) icebergTable).operations();
    }
    throw new UnsupportedOperationException();
  }

  @Override
  public StructLikeMap<Map<String, String>> partitionProperty() {
    String s = icebergTable.properties().get(TableProperties.TABLE_PARTITION_PROPERTIES);
    if (s != null) {
      return TablePropertyUtil.decodePartitionProperties(spec(), s);
    } else {
      return StructLikeMap.create(spec().partitionType());
    }
  }

  @Override
  public UpdatePartitionProperties updatePartitionProperties(Transaction transaction) {
    return new PartitionPropertiesUpdate(this, transaction);
  }

  @Override
  public long beginTransaction(String signature) {
    if (client == null) {
      throw new UnsupportedOperationException("Ams client is null");
    }
    try {
      return client.allocateTransactionId(tableIdentifier.buildTableIdentifier(), signature);
    } catch (TException e) {
      throw new IllegalStateException("failed begin transaction", e);
    }
  }
}
