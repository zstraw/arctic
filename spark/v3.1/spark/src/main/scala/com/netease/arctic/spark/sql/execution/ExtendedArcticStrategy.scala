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

package com.netease.arctic.spark.sql.execution

import com.netease.arctic.spark.SparkSQLProperties
import com.netease.arctic.spark.sql.ArcticExtensionUtils.{ArcticTableHelper, isArcticCatalog, isArcticTable}
import com.netease.arctic.spark.sql.catalyst.plans._
import com.netease.arctic.spark.table.ArcticSparkTable
import com.netease.arctic.spark.writer.WriteMode
import org.apache.spark.sql.catalyst.analysis.{NamedRelation, ResolvedTable}
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.catalyst.plans.logical.{CreateTableAsSelect, DescribeRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.utils.TranslateUtils
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.CreateTableLikeCommand
import org.apache.spark.sql.execution.datasources.v2
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Implicits.TableHelper
import org.apache.spark.sql.execution.datasources.v2._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.{SparkSession, Strategy}

import scala.collection.JavaConverters
import scala.collection.JavaConverters.mapAsJavaMapConverter

case class ExtendedArcticStrategy(spark: SparkSession) extends Strategy with PredicateHelper{

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case CreateTableAsSelect(catalog, ident, parts, query, props, options, ifNotExists)
      if isArcticCatalog(catalog) =>
        var propertiesMap: Map[String, String] = props
        var optionsMap: Map[String, String] = options
        if (options.contains("primary.keys")) {
          propertiesMap += ("primary.keys" -> options("primary.keys"))
        }
        if(propertiesMap.contains("primary.keys")) {
          optionsMap += (WriteMode.WRITE_MODE_KEY -> WriteMode.OVERWRITE_DYNAMIC.mode)
        }

      val writeOptions = new CaseInsensitiveStringMap(JavaConverters.mapAsJavaMap(optionsMap))
      CreateTableAsSelectExec(catalog, ident, parts, query, planLater(query),
        propertiesMap, writeOptions, ifNotExists) :: Nil

    case CreateTableLikeCommand(targetTable, sourceTable, storage, provider, properties, ifNotExists)
      if provider.get != null && provider.get.equals("arctic")=>
        CreateArcticTableLikeExec(spark, targetTable, sourceTable, storage, provider, properties, ifNotExists) :: Nil

    case CreateArcticTableAsSelect(catalog, ident, parts, query, validateQuery, props, options, ifNotExists)
      if isArcticCatalog(catalog) =>
        val writeOptions = new CaseInsensitiveStringMap(options.asJava)
        CreateArcticTableAsSelectExec(catalog, ident, parts, query, planLater(query), planLater(validateQuery),
          props, writeOptions, ifNotExists) :: Nil

    case DescribeRelation(r: ResolvedTable, partitionSpec, isExtended)
      if isArcticTable(r.table) =>
        if (partitionSpec.nonEmpty) {
          throw new RuntimeException("DESCRIBE does not support partition for v2 tables.")
        }
        DescribeKeyedTableExec(r.table, r.catalog, r.identifier, isExtended) :: Nil

    case MigrateToArcticLogicalPlan(command) =>
      println("create migrate to arctic command logical")
      MigrateToArcticExec(command)::Nil

    case ReplaceArcticData(d: DataSourceV2Relation, query, options) =>
      AppendDataExec(d.table.asWritable, new CaseInsensitiveStringMap(options.asJava), planLater(query), refreshCache(d)) :: Nil

    case AppendArcticData(d: DataSourceV2Relation, query, validateQuery, options) =>
      d.table match {
        case t: ArcticSparkTable =>
          AppendInsertDataExec(t, new CaseInsensitiveStringMap(options.asJava), planLater(query),
            planLater(validateQuery), refreshCache(d)) :: Nil
        case table =>
          throw new UnsupportedOperationException(s"Cannot append data to non-Arctic table: $table")
      }

    case ArcticRowLevelWrite(table: DataSourceV2Relation, query, options, projs) =>
      ArcticRowLevelWriteExec(table.table.asArcticTable, planLater(query),
        new CaseInsensitiveStringMap(options.asJava), projs, refreshCache(table)) :: Nil

    case OverwriteArcticData(d: DataSourceV2Relation, query, validateQuery, options) =>
      d.table match {
        case t: ArcticSparkTable =>
          OverwriteArcticDataExec(t, new CaseInsensitiveStringMap(options.asJava), planLater(query),
            planLater(validateQuery), refreshCache(d)) :: Nil
        case table =>
          throw new UnsupportedOperationException(s"Cannot overwrite to non-Arctic table: $table")
      }

    case OverwriteArcticDataByExpression(d: DataSourceV2Relation, deleteExpr, query, validateQuery, options) =>
      d.table match {
        case t: ArcticSparkTable =>
          val filters = splitConjunctivePredicates(deleteExpr).map {
            filter =>
              TranslateUtils.translateFilter(deleteExpr).getOrElse(
                throw new UnsupportedOperationException("Cannot translate expression to source filter"))
          }.toArray
          OverwriteArcticByExpressionExec(t, filters, new CaseInsensitiveStringMap(options.asJava), planLater(query),
            planLater(validateQuery), refreshCache(d)) :: Nil
        case table =>
          throw new UnsupportedOperationException(s"Cannot overwrite by filter to non-Arctic table: $table")
      }

    case MergeRows(isSourceRowPresent, isTargetRowPresent, matchedConditions, matchedOutputs, notMatchedConditions,
    notMatchedOutputs, rowIdAttrs, matchedRowCheck, unMatchedRowCheck, emitNotMatchedTargetRows,
    output, child) =>
      MergeRowsExec(isSourceRowPresent, isTargetRowPresent, matchedConditions, matchedOutputs, notMatchedConditions,
        notMatchedOutputs, rowIdAttrs, matchedRowCheck, unMatchedRowCheck, emitNotMatchedTargetRows,
        output, planLater(child)) :: Nil

    case d@AlterArcticTableDropPartition(r: ResolvedTable, _, _, _, _) =>
      AlterArcticTableDropPartitionExec(r.table, d.parts, d.retainData):: Nil

    case TruncateArcticTable(r: ResolvedTable, spec) =>
      TruncateArcticTableExec(r.table, spec) :: Nil

    case _ => Nil
  }

  private def refreshCache(r: NamedRelation)(): Unit = {
    spark.sharedState.cacheManager.recacheByPlan(spark, r)
  }

}
