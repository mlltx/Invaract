// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invariant Contributors

package com.example.sparkadapter

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/** The known-safe exemption list for `ContractEnforcementRule`'s fail-closed
  * policy on unverifiable writes (see that object's "Fail-closed on
  * unverifiable writes" doc). A `Command`-shaped plan not translated to
  * `ir.Write` is rejected *unless* its concrete class is listed here.
  *
  * ## How this list was built
  *
  * Not guessed: every concrete class implementing
  * `org.apache.spark.sql.catalyst.plans.logical.Command` in Spark 3.5.1's
  * `spark-sql`/`spark-catalyst` jars and Delta 3.2.0's `delta-spark` jar was
  * enumerated by reflectively scanning those jars (`JarFile` entries +
  * `Class.forName` + `Command.isAssignableFrom`), then each one classified
  * by hand against its documented SQL semantics. 164 classes were found;
  * the ones below were judged not to change a table's *committed row
  * content* (the only thing a contract's output section can meaningfully
  * be checked against) and so are exempt from the fail-closed check.
  *
  * This is a judgment call, not a re-verification of every class's actual
  * runtime behavior (that would mean ~150 more probe tests, one per
  * command) - see docs/SPARK_ADAPTER.md's "Fail-closed on unverifiable
  * writes" section for the full reasoning and the explicit list of
  * data-mutating commands (MERGE/DELETE/UPDATE/LOAD DATA/TRUNCATE/RESTORE/
  * CLONE/...) deliberately left *off* this list, so they fail closed until
  * `SparkPlanAdapter` gains a real translation for them. Being wrong in
  * that direction (a safe command missing from this list) just means one
  * extra rejection until someone adds it; being wrong the other way (an
  * unsafe command wrongly listed here) would silently defeat the whole
  * point of this feature, so uncertain cases were left off deliberately.
  *
  * Matched by fully-qualified class name (a `Set[String]`, not
  * `classOf[...]`/`isInstanceOf`) for the same reason `SparkPlanAdapter`'s
  * `jdbcLocationOf`/`unwrapWriteWrapper` use class-name string matching:
  * about a sixth of these classes live in `org.apache.spark.sql.delta`,
  * which this module has no compile-time dependency on (see
  * docs/SPARK_ADAPTER.md's "Delta Lake support" section) - importing them
  * directly would reintroduce exactly the dependency this module was built
  * to avoid.
  */
private[sparkadapter] object FailClosedCommands {

  def isKnownSafe(plan: LogicalPlan): Boolean = knownSafe.contains(plan.getClass.getName)

  private val knownSafe: Set[String] = Set(
    // -- Storage maintenance: rewrites/removes files but doesn't change a
    // table's current logical row content.
    "io.delta.tables.execution.VacuumTableCommand",
    "org.apache.spark.sql.delta.commands.OptimizeTableCommand",

    // -- catalyst DDL/catalog-metadata commands (schema, namespace,
    // function, index, view, cache, partition *registration* - not data)
    "org.apache.spark.sql.catalyst.plans.logical.AddColumns",
    "org.apache.spark.sql.catalyst.plans.logical.AddPartitions",
    "org.apache.spark.sql.catalyst.plans.logical.AlterColumn",
    "org.apache.spark.sql.catalyst.plans.logical.AlterTableAddConstraint",
    "org.apache.spark.sql.catalyst.plans.logical.AlterTableDropConstraint",
    "org.apache.spark.sql.catalyst.plans.logical.AlterTableDropFeature",
    "org.apache.spark.sql.catalyst.plans.logical.AlterViewAs",
    "org.apache.spark.sql.catalyst.plans.logical.AnalyzeColumn",
    "org.apache.spark.sql.catalyst.plans.logical.AnalyzeTable",
    "org.apache.spark.sql.catalyst.plans.logical.AnalyzeTables",
    "org.apache.spark.sql.catalyst.plans.logical.CacheTable",
    "org.apache.spark.sql.catalyst.plans.logical.CacheTableAsSelect",
    "org.apache.spark.sql.catalyst.plans.logical.CommentOnNamespace",
    "org.apache.spark.sql.catalyst.plans.logical.CommentOnTable",
    "org.apache.spark.sql.catalyst.plans.logical.CreateFunction",
    "org.apache.spark.sql.catalyst.plans.logical.CreateIndex",
    "org.apache.spark.sql.catalyst.plans.logical.CreateNamespace",
    "org.apache.spark.sql.catalyst.plans.logical.CreateTable", // plain CREATE TABLE (no AS SELECT) - CreateTableAsSelect is a recognized write
    "org.apache.spark.sql.catalyst.plans.logical.CreateView",
    "org.apache.spark.sql.catalyst.plans.logical.DescribeColumn",
    "org.apache.spark.sql.catalyst.plans.logical.DescribeFunction",
    "org.apache.spark.sql.catalyst.plans.logical.DescribeNamespace",
    "org.apache.spark.sql.catalyst.plans.logical.DescribeRelation",
    "org.apache.spark.sql.catalyst.plans.logical.DropColumns",
    "org.apache.spark.sql.catalyst.plans.logical.DropFunction",
    "org.apache.spark.sql.catalyst.plans.logical.DropIndex",
    "org.apache.spark.sql.catalyst.plans.logical.DropNamespace",
    "org.apache.spark.sql.catalyst.plans.logical.DropView",
    "org.apache.spark.sql.catalyst.plans.logical.NoopCommand",
    "org.apache.spark.sql.catalyst.plans.logical.RecoverPartitions", // MSCK REPAIR-style discovery of existing partitions, writes no data
    "org.apache.spark.sql.catalyst.plans.logical.RefreshFunction",
    "org.apache.spark.sql.catalyst.plans.logical.RefreshTable",
    "org.apache.spark.sql.catalyst.plans.logical.RenameColumn",
    "org.apache.spark.sql.catalyst.plans.logical.RenamePartitions",
    "org.apache.spark.sql.catalyst.plans.logical.RenameTable",
    "org.apache.spark.sql.catalyst.plans.logical.RepairTable",
    "org.apache.spark.sql.catalyst.plans.logical.ReplaceColumns",
    "org.apache.spark.sql.catalyst.plans.logical.SetCatalogAndNamespace",
    "org.apache.spark.sql.catalyst.plans.logical.SetNamespaceLocation",
    "org.apache.spark.sql.catalyst.plans.logical.SetNamespaceProperties",
    "org.apache.spark.sql.catalyst.plans.logical.SetTableLocation",
    "org.apache.spark.sql.catalyst.plans.logical.SetTableProperties",
    "org.apache.spark.sql.catalyst.plans.logical.SetTableSerDeProperties",
    "org.apache.spark.sql.catalyst.plans.logical.SetViewProperties",
    "org.apache.spark.sql.catalyst.plans.logical.ShowColumns",
    "org.apache.spark.sql.catalyst.plans.logical.ShowCreateTable",
    "org.apache.spark.sql.catalyst.plans.logical.ShowFunctions",
    "org.apache.spark.sql.catalyst.plans.logical.ShowNamespaces",
    "org.apache.spark.sql.catalyst.plans.logical.ShowPartitions",
    "org.apache.spark.sql.catalyst.plans.logical.ShowTableExtended",
    "org.apache.spark.sql.catalyst.plans.logical.ShowTableProperties",
    "org.apache.spark.sql.catalyst.plans.logical.ShowTables",
    "org.apache.spark.sql.catalyst.plans.logical.ShowViews",
    "org.apache.spark.sql.catalyst.plans.logical.UncacheTable",
    "org.apache.spark.sql.catalyst.plans.logical.UnsetTableProperties",
    "org.apache.spark.sql.catalyst.plans.logical.UnsetViewProperties",

    // -- Delta ALTER TABLE / describe / clustering-config commands - schema
    // and table-properties metadata only, no row content change.
    "org.apache.spark.sql.delta.commands.AlterTableAddColumnsDeltaCommand",
    "org.apache.spark.sql.delta.commands.AlterTableAddConstraintDeltaCommand",
    "org.apache.spark.sql.delta.commands.AlterTableChangeColumnDeltaCommand",
    "org.apache.spark.sql.delta.commands.AlterTableClusterByDeltaCommand",
    "org.apache.spark.sql.delta.commands.AlterTableDropColumnsDeltaCommand",
    "org.apache.spark.sql.delta.commands.AlterTableDropConstraintDeltaCommand",
    "org.apache.spark.sql.delta.commands.AlterTableDropFeatureDeltaCommand",
    "org.apache.spark.sql.delta.commands.AlterTableReplaceColumnsDeltaCommand",
    "org.apache.spark.sql.delta.commands.AlterTableSetLocationDeltaCommand",
    "org.apache.spark.sql.delta.commands.AlterTableSetPropertiesDeltaCommand",
    "org.apache.spark.sql.delta.commands.AlterTableUnsetPropertiesDeltaCommand",
    "org.apache.spark.sql.delta.commands.DescribeDeltaDetailCommand",
    "org.apache.spark.sql.delta.commands.DescribeDeltaHistoryCommand",
    "org.apache.spark.sql.delta.commands.ShowDeltaTableColumnsCommand",
    "org.apache.spark.sql.delta.skipping.clustering.temp.AlterTableClusterBy",

    // -- Spark execution-layer DDL/catalog/session commands - same
    // reasoning as the catalyst DDL group above.
    "org.apache.spark.sql.execution.command.AddArchivesCommand",
    "org.apache.spark.sql.execution.command.AddFilesCommand",
    "org.apache.spark.sql.execution.command.AddJarsCommand",
    "org.apache.spark.sql.execution.command.AlterDatabasePropertiesCommand",
    "org.apache.spark.sql.execution.command.AlterDatabaseSetLocationCommand",
    "org.apache.spark.sql.execution.command.AlterTableAddColumnsCommand",
    "org.apache.spark.sql.execution.command.AlterTableAddPartitionCommand", // registers a partition spec; writes no data itself
    "org.apache.spark.sql.execution.command.AlterTableChangeColumnCommand",
    "org.apache.spark.sql.execution.command.AlterTableRenameCommand",
    "org.apache.spark.sql.execution.command.AlterTableRenamePartitionCommand",
    "org.apache.spark.sql.execution.command.AlterTableSerDePropertiesCommand",
    "org.apache.spark.sql.execution.command.AlterTableSetLocationCommand",
    "org.apache.spark.sql.execution.command.AlterTableSetPropertiesCommand",
    "org.apache.spark.sql.execution.command.AlterTableUnsetPropertiesCommand",
    "org.apache.spark.sql.execution.command.AlterViewAsCommand",
    "org.apache.spark.sql.execution.command.AnalyzeColumnCommand",
    "org.apache.spark.sql.execution.command.AnalyzePartitionCommand",
    "org.apache.spark.sql.execution.command.AnalyzeTableCommand",
    "org.apache.spark.sql.execution.command.AnalyzeTablesCommand",
    "org.apache.spark.sql.execution.command.CreateDataSourceTableCommand", // schema-only (no AS SELECT) - confirmed empirically, see docs/SPARK_ADAPTER.md
    "org.apache.spark.sql.execution.command.CreateDatabaseCommand",
    "org.apache.spark.sql.execution.command.CreateFunctionCommand",
    "org.apache.spark.sql.execution.command.CreateTableCommand",
    "org.apache.spark.sql.execution.command.CreateTableLikeCommand", // new table, same schema, no data copied
    "org.apache.spark.sql.execution.command.CreateViewCommand",
    "org.apache.spark.sql.execution.command.DescribeColumnCommand",
    "org.apache.spark.sql.execution.command.DescribeDatabaseCommand",
    "org.apache.spark.sql.execution.command.DescribeFunctionCommand",
    "org.apache.spark.sql.execution.command.DescribeQueryCommand",
    "org.apache.spark.sql.execution.command.DescribeTableCommand",
    "org.apache.spark.sql.execution.command.DropFunctionCommand",
    "org.apache.spark.sql.execution.command.DropTempViewCommand", // a temp view has no persisted data to lose
    "org.apache.spark.sql.execution.command.ExplainCommand", // never executes the command it explains
    "org.apache.spark.sql.execution.command.ListArchivesCommand",
    "org.apache.spark.sql.execution.command.ListFilesCommand",
    "org.apache.spark.sql.execution.command.ListJarsCommand",
    "org.apache.spark.sql.execution.command.RefreshFunctionCommand",
    "org.apache.spark.sql.execution.command.RefreshTableCommand",
    "org.apache.spark.sql.execution.command.RepairTableCommand",
    "org.apache.spark.sql.execution.command.ResetCommand",
    "org.apache.spark.sql.execution.command.SetCatalogCommand",
    "org.apache.spark.sql.execution.command.SetCommand",
    "org.apache.spark.sql.execution.command.SetNamespaceCommand",
    "org.apache.spark.sql.execution.command.ShowCatalogsCommand",
    "org.apache.spark.sql.execution.command.ShowColumnsCommand",
    "org.apache.spark.sql.execution.command.ShowCreateTableAsSerdeCommand",
    "org.apache.spark.sql.execution.command.ShowCreateTableCommand",
    "org.apache.spark.sql.execution.command.ShowCurrentNamespaceCommand",
    "org.apache.spark.sql.execution.command.ShowFunctionsCommand",
    "org.apache.spark.sql.execution.command.ShowPartitionsCommand",
    "org.apache.spark.sql.execution.command.ShowTablePropertiesCommand",
    "org.apache.spark.sql.execution.command.ShowTablesCommand",
    "org.apache.spark.sql.execution.command.ShowViewsCommand",
    "org.apache.spark.sql.execution.command.StreamingExplainCommand",

    // -- datasources package - metadata/cache only.
    "org.apache.spark.sql.execution.datasources.CreateTempViewUsing",
    "org.apache.spark.sql.execution.datasources.RefreshResource",

    // -- Iceberg 1.11.0's own SQL-extension commands (found via the same
    // reflective-jar-scan technique as Delta's, this time against
    // iceberg-spark-runtime-3.5_2.12) - all thirteen are metadata/ref
    // operations, never row content: branch/tag create-or-replace/drop
    // manage a *named pointer* to an existing, immutable snapshot (the
    // same reasoning a git branch/tag ref would get - creating, moving,
    // or deleting the pointer doesn't touch any commit's actual content);
    // partition-spec and identifier-field evolution change how *future*
    // writes are organized, not any already-committed row; write-
    // distribution/ordering is a write-planning hint; view create/drop/
    // show are SQL view definitions, no data of their own (matching
    // this list's existing view-command entries above). See
    // docs/SPARK_ADAPTER.md's Iceberg section for the full reasoning.
    "org.apache.spark.sql.catalyst.plans.logical.AddPartitionField",
    "org.apache.spark.sql.catalyst.plans.logical.CreateOrReplaceBranch",
    "org.apache.spark.sql.catalyst.plans.logical.CreateOrReplaceTag",
    "org.apache.spark.sql.catalyst.plans.logical.DropBranch",
    "org.apache.spark.sql.catalyst.plans.logical.DropIdentifierFields",
    "org.apache.spark.sql.catalyst.plans.logical.DropPartitionField",
    "org.apache.spark.sql.catalyst.plans.logical.DropTag",
    "org.apache.spark.sql.catalyst.plans.logical.ReplacePartitionField",
    "org.apache.spark.sql.catalyst.plans.logical.SetIdentifierFields",
    "org.apache.spark.sql.catalyst.plans.logical.SetWriteDistributionAndOrdering",
    "org.apache.spark.sql.catalyst.plans.logical.views.CreateIcebergView",
    "org.apache.spark.sql.catalyst.plans.logical.views.DropIcebergView",
    "org.apache.spark.sql.catalyst.plans.logical.views.ShowIcebergViews"

    // Deliberately NOT listed (fails closed until SparkPlanAdapter
    // translates them, or someone confirms they're safe and adds them
    // here with the same reasoning): anything that adds, modifies, drops,
    // or copies row content - DeleteFromTable(WithFilters), MergeIntoTable/
    // DeltaMergeInto, UpdateTable, LoadData(Command), TruncateTable(Command
    // /Partition), DropTable(Command)/DropDatabaseCommand (can delete a
    // managed table's/database's data), DropPartitions/
    // AlterTableDropPartitionCommand (deletes the partition's data),
    // ReplaceTable (drops+recreates), Delta's WriteIntoDelta/
    // CloneTableCommand/ConvertToDeltaCommand/CreateDeltaTableCommand/
    // DeltaReorgTable(Command)/DeltaGenerateCommand/RestoreTableCommand,
    // InsertIntoDataSourceCommand/InsertIntoDataSourceDirCommand (real
    // writes, just not yet translated), and ExternalCommandExecutor
    // (arbitrary passthrough to an external system - can't be classified
    // either way). See docs/SPARK_ADAPTER.md's "Fail-closed on
    // unverifiable writes" section for the per-class reasoning.
    //
    // Delta's DeleteCommand/UpdateCommand/MergeIntoCommand are no longer
    // in that "not yet translated" bucket - WriteCommandSupport's
    // deltaRowLevelDml case recognizes all three by reflection, so they
    // never reach this class's check at all now (that's why they're not
    // in the exclusion list above either - they're neither known-safe
    // nor unhandled). See that case's own doc for exactly what it does
    // and, just as importantly, doesn't verify about them.
    //
    // Iceberg's ReplaceData/WriteDelta (its MERGE/UPDATE/DELETE mechanism)
    // are the same story, via WriteCommandSupport's dsv2RowLevelWrite case
    // - recognized directly (no reflection needed, unlike Delta's), so
    // they too never reach this check.
    //
    // Iceberg's Call (org.apache.spark.sql.catalyst.plans.logical.Call) is
    // deliberately excluded from the safe list above, unlike its other
    // thirteen SQL-extension commands - confirmed empirically that this
    // one class represents *every* `CALL <catalog>.system.<proc>(...)`
    // procedure (rewrite_data_files, expire_snapshots,
    // rollback_to_snapshot, add_files, migrate, ...) uniformly, with no
    // structural way to tell which procedure a given instance invokes
    // without inspecting its runtime arguments - and those procedures
    // span the full range from genuinely safe (expire_snapshots removes
    // only unreferenced metadata/orphaned files) to genuinely
    // row-content-mutating (rollback_to_snapshot can revert what "current"
    // data is). Safe-listing the class would silently pass ALL of them,
    // including the mutating ones - exactly the asymmetry this list's own
    // header warns against. So every CALL fails closed today, including
    // the harmless ones, until a future pass adds procedure-name-aware
    // classification (a genuinely bigger feature - see
    // docs/ADDING_A_SPARK_CONNECTOR.md's "Known limitations").
  )
}
