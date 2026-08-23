// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invariant Contributors

package com.example.sparkadapter

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.CreateDataSourceTableAsSelectCommand
import org.apache.spark.sql.execution.datasources.{InsertIntoHadoopFsRelationCommand, SaveIntoDataSourceCommand}
import org.apache.spark.sql.types.StructType

/** Everything needed to represent one Spark write command as `ir.Write`:
  * where it writes, the (untranslated) plan being written, its format and
  * save mode, the schema to verify a contract's output against, and an
  * optional diagnostic if something had to be guessed. `query` is left
  * untranslated — the actual recursive translation (and diagnostic
  * accumulation for the *rest* of the plan tree) stays with
  * `SparkPlanAdapter.Translator`; this is pure, stateless extraction from
  * one command node, reusable by callers that don't want a full
  * translation (see `ContractEnforcementRule`/`SparkAdapterListener`
  * below).
  */
private[sparkadapter] case class WriteCommandInfo(
  location: String,
  query: LogicalPlan,
  format: Option[String],
  saveMode: Option[String],
  outputSchema: StructType,
  diagnostic: Option[Diagnostic] = None
)

/** One entry per Spark write-command *shape* this module recognizes — see
  * docs/SPARK_ADAPTER.md's "Translation coverage" for what each of the
  * three shapes below actually is, and docs/ADDING_A_SPARK_CONNECTOR.md
  * for the process a new connector follows to potentially add a fourth.
  *
  * ## Why this exists
  *
  * Before this, "is this plan a write, and what does it mean" was
  * implemented three separate times: once in
  * `SparkPlanAdapter.Translator.translatePlan` (to actually translate
  * it), once in `ContractEnforcementRule.verifyOrThrow` (just to pull the
  * right output schema — a `Command` node's own `.schema` is empty, so
  * this needs the underlying query's), and once in
  * `SparkAdapterListener.onSuccess` (just to decide whether to capture a
  * write for `demo/output/report.json`). Three independent
  * `case cmd: X =>` matches, hand-kept in lockstep. Both of this module's
  * real Delta-support bugs were exactly a write shape added to
  * `translatePlan` and missed in one of the other two matches — not a
  * one-off mistake, a structural hazard built into having the same fact
  * ("is this a write, and what schema does it write") encoded three
  * times.
  *
  * `combined` is now the single source of truth all three sites consult.
  * Adding a write shape here reaches every site that needs it
  * automatically — there is no second match to remember, and the
  * `WriteCommandInfo` shape makes it structurally impossible to translate
  * a write without also supplying the schema `ContractEnforcementRule`
  * needs, the way the original Delta bug did.
  */
private[sparkadapter] object WriteCommandSupport {

  private val insertIntoHadoopFsRelation: PartialFunction[LogicalPlan, WriteCommandInfo] = {
    case cmd: InsertIntoHadoopFsRelationCommand =>
      WriteCommandInfo(
        location = cmd.outputPath.toString,
        query = unwrapWriteWrapper(cmd.query),
        format = SparkPlanAdapter.formatOf(cmd.fileFormat),
        saveMode = SparkPlanAdapter.saveModeOf(cmd.mode),
        outputSchema = cmd.query.schema
      )
  }

  // Delta Lake (and any other CreatableRelationProvider-based source
  // written via `.save(...)` rather than Spark's FileFormat-based write
  // path above) goes through this command instead of
  // InsertIntoHadoopFsRelationCommand - confirmed empirically against a
  // real Delta-enabled session, not assumed (see docs/SPARK_ADAPTER.md's
  // "Delta Lake support" section): `df.write.format("delta").save(path)`
  // analyzes to exactly this node, with Delta's own DeltaDataSource as
  // `dataSource`. No connector-specific code or dependency is needed to
  // translate it: SaveIntoDataSourceCommand and DataSourceRegister are
  // both plain, public spark-sql classes already on this module's
  // existing Spark dependency, and any CreatableRelationProvider that
  // also mixes in DataSourceRegister (as Delta's does, and as JDBC's
  // does) gets a precise format for free via `formatOf`.
  private val saveIntoDataSource: PartialFunction[LogicalPlan, WriteCommandInfo] = {
    case cmd: SaveIntoDataSourceCommand =>
      val (location, diagnostic) = cmd.options.get("path") match {
        case Some(path) => (path, None)
        case None =>
          val msg = s"No 'path' option on a ${cmd.dataSource.getClass.getSimpleName} write; " +
            "using its options map as a best-effort location"
          (cmd.options.toString, Some(Diagnostic("SaveIntoDataSourceCommand", msg)))
      }
      WriteCommandInfo(
        location = location,
        query = cmd.query,
        format = SparkPlanAdapter.formatOf(cmd.dataSource),
        saveMode = SparkPlanAdapter.saveModeOf(cmd.mode),
        outputSchema = cmd.query.schema,
        diagnostic = diagnostic
      )
  }

  // `.saveAsTable(...)`/`CREATE TABLE ... USING <format> AS SELECT ...`
  // against a *new* V1 data source table - confirmed empirically (see
  // docs/SPARK_ADAPTER.md's "Fail-closed on unverifiable writes" section):
  // analyzes to this command wrapping the actual data write, distinct
  // from both shapes above. `table.provider` is already the clean format
  // string `formatOf` derives from `DataSourceRegister` elsewhere - no
  // lookup needed here.
  private val createDataSourceTableAsSelect: PartialFunction[LogicalPlan, WriteCommandInfo] = {
    case cmd: CreateDataSourceTableAsSelectCommand =>
      val (location, diagnostic) = cmd.table.storage.locationUri match {
        case Some(uri) => (uri.toString, None)
        case None =>
          val msg = s"No storage location on new table '${cmd.table.identifier}'; " +
            "using its table identifier as a best-effort location"
          (cmd.table.identifier.unquotedString, Some(Diagnostic("CreateDataSourceTableAsSelectCommand", msg)))
      }
      WriteCommandInfo(
        location = location,
        query = cmd.query,
        format = cmd.table.provider,
        saveMode = SparkPlanAdapter.saveModeOf(cmd.mode),
        outputSchema = cmd.query.schema,
        diagnostic = diagnostic
      )
  }

  /** Every recognized write shape, combined into one lookup —
    * `SparkPlanAdapter.Translator.translatePlan`,
    * `ContractEnforcementRule.verifyOrThrow`, and
    * `SparkAdapterListener.onSuccess` all consult exactly this, never a
    * match of their own. Add a new connector's write shape (when it
    * actually needs one — most don't; see docs/ADDING_A_SPARK_CONNECTOR.md)
    * by adding one more `PartialFunction` above, following the three as
    * templates, and chaining it in here. Nothing else in this module
    * needs to change.
    */
  val combined: PartialFunction[LogicalPlan, WriteCommandInfo] =
    insertIntoHadoopFsRelation orElse saveIntoDataSource orElse createDataSourceTableAsSelect

  /** Spark 3.4+ inserts an internal `WriteFiles` wrapper between a write
    * command and its query in the optimized/analyzed plan (confirmed
    * empirically for 3.5.1 — see docs/SPARK_ADAPTER.md). It carries no
    * information relevant to this IR, so it's unwrapped by class name
    * rather than importing it directly: an internal class an adapter
    * targeting a different Spark version might not have. Only
    * `InsertIntoHadoopFsRelationCommand`'s query needs this — confirmed
    * empirically that the other two shapes' queries don't get this
    * wrapper inserted.
    */
  private def unwrapWriteWrapper(plan: LogicalPlan): LogicalPlan =
    if (plan.getClass.getSimpleName == "WriteFiles" && plan.children.size == 1) plan.children.head
    else plan
}
