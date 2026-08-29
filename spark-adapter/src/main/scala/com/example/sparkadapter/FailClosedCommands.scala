// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

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

  def isKnownSafe(plan: LogicalPlan): Boolean =
    if (plan.getClass.getName == icebergCallClassName) isKnownSafeIcebergProcedureCall(plan)
    else knownSafe.contains(plan.getClass.getName)

  // Iceberg's Call node (see the "Iceberg CALL procedures" comment at the
  // bottom of the knownSafe set below for the full background) always has this one
  // concrete class regardless of which `CALL system.<proc>(...)` procedure
  // was invoked - so, uniquely among everything else in this file,
  // class-name matching on the *plan itself* can't distinguish safe from
  // unsafe. What can: `Call.procedure(): Procedure` is a real, concrete,
  // per-procedure-type class (confirmed via javap - `SparkProcedures`'
  // builder registry instantiates a distinct class per procedure name,
  // e.g. `RewriteDataFilesProcedure`, no dynamic proxy involved), reachable
  // via reflection the same way `WriteCommandSupport`'s `deltaRowLevelDml`
  // reflects into Delta's MERGE/UPDATE/DELETE commands. Any reflection
  // failure (a future Iceberg version reshaping `Call`) falls back to
  // `false` - fails closed, never silently safe.
  private val icebergCallClassName = "org.apache.spark.sql.catalyst.plans.logical.Call"

  private[sparkadapter] def isKnownSafeIcebergProcedureCall(plan: LogicalPlan): Boolean =
    try {
      val procedure = plan.getClass.getMethod("procedure").invoke(plan)
      safeIcebergProcedureClasses.contains(procedure.getClass.getName)
    } catch {
      case _: Throwable => false
    }

  // Classified by hand against each procedure's actual delegate action
  // class (confirmed via javap, not guessed - e.g. RewriteDataFilesProcedure
  // delegates to org.apache.iceberg.actions.RewriteDataFiles) and Iceberg's
  // own documentation for the ones whose semantics weren't obvious from the
  // class name alone. Every procedure here is either pure metadata/
  // statistics, storage-layout compaction that preserves the same logical
  // rows, GC of files/snapshots already unreferenced by anything live, or
  // read-only introspection - see docs/SPARK_ADAPTER.md's "Iceberg CALL
  // procedure classification" section for the full per-procedure reasoning,
  // including the ones deliberately left off this list (most of which have
  // real verification via StateChangingCallSupport instead, not a blanket
  // rejection - see that file's own doc for which).
  private val safeIcebergProcedureClasses: Set[String] = Set(
    // -- Storage/metadata compaction: rewrites files, preserves the same
    // logical rows. Same category as Delta's OptimizeTableCommand above.
    "org.apache.iceberg.spark.procedures.RewriteDataFilesProcedure",
    "org.apache.iceberg.spark.procedures.RewriteManifestsProcedure",
    "org.apache.iceberg.spark.procedures.RewritePositionDeleteFilesProcedure",
    // -- GC of files/snapshots already unreferenced by any live table
    // state. Same category as Delta's VacuumTableCommand above.
    "org.apache.iceberg.spark.procedures.RemoveOrphanFilesProcedure",
    "org.apache.iceberg.spark.procedures.ExpireSnapshotsProcedure",
    // -- Catalog/stats/introspection: no row content touched anywhere.
    "org.apache.iceberg.spark.procedures.RegisterTableProcedure",
    "org.apache.iceberg.spark.procedures.AncestorsOfProcedure",
    "org.apache.iceberg.spark.procedures.ComputeTableStatsProcedure",
    "org.apache.iceberg.spark.procedures.ComputePartitionStatsProcedure",
    "org.apache.iceberg.spark.procedures.CreateChangelogViewProcedure",
    // -- rewrite_table_path: writes a portable copy of metadata/data file
    // references at a target path prefix, for physically relocating a
    // table's storage - confirmed via a real probe (since deleted) that it
    // never touches the SOURCE table's own catalog entry, current schema,
    // or current snapshot, and registers no new catalog table itself (an
    // external process is expected to do that later, against the copy).
    // Nothing a contract could ever check is affected - same category as
    // the read-only/GC procedures above, not "genuinely data-mutating but
    // unmodeled" the way the CALL-argument-parsing procedures below are.
    "org.apache.iceberg.spark.procedures.RewriteTablePathProcedure"
  )

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
    // deliberately excluded from this class-name set, unlike its other
    // thirteen SQL-extension commands - it's the one Spark class every
    // `CALL <catalog>.system.<proc>(...)` procedure shares, so class-name
    // matching on the plan itself can't distinguish safe from unsafe
    // procedures. `isKnownSafe` special-cases it: see
    // `isKnownSafeIcebergProcedureCall`/`safeIcebergProcedureClasses` above
    // for per-*procedure* (not per-plan-class) classification, matched on
    // `Call.procedure()`'s own concrete class via reflection. Ten of
    // Iceberg's twenty system procedures are classified safe there
    // (storage/metadata compaction, GC of already-unreferenced
    // files/snapshots, catalog registration, stats, read-only
    // introspection); the other ten - rollback_to_snapshot/
    // rollback_to_timestamp/set_current_snapshot (change which snapshot is
    // "current"), cherrypick_snapshot/publish_changes/fast_forward (apply
    // or fast-forward to a different snapshot's changes), add_files
    // (imports external files as new rows), migrate (converts an existing
    // table's format in place), snapshot/rewrite_table_path (produce new
    // persisted table content, even though neither modifies its *source*
    // table) - stay unmodeled and fail closed, deliberately: each would
    // need its own new verification mechanism (a target snapshot's
    // already-recorded schema read from the catalog, or a schema read from
    // a CALL argument's referenced table/path - neither is "translate a
    // Spark write," the model every other case here fits), tracked as
    // separate future work, not attempted in this pass. See
    // docs/SPARK_ADAPTER.md's "Iceberg CALL procedure classification"
    // section for the full per-procedure reasoning.
  )
}
