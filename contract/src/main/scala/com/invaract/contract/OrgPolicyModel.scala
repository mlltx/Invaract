// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.contract

import java.time.LocalDate
import scala.collection.JavaConverters._

/** Raised when an organizational policy document cannot be parsed into the
  * object model. See `ContractParseException`'s analogous role for
  * contracts.
  */
class OrgPolicyParseException(message: String, cause: Throwable = null) extends RuntimeException(message, cause)

/** Whether a policy rule blocks (`Enforce`) or merely reports (`Warn`) a
  * violation. `Warn` exists so a platform can roll out a new org-wide
  * policy safely — watch violations accumulate via the notification sink
  * for a burn-down period before flipping the same rule to `Enforce` — a
  * single YAML edit, no code change on any governed job.
  */
sealed trait PolicyMode
object PolicyMode {
  case object Enforce extends PolicyMode
  case object Warn extends PolicyMode
}

/** Which of a contract's datasets a policy rule applies to. */
sealed trait PolicyScope
object PolicyScope {
  case object Inputs extends PolicyScope
  case object Outputs extends PolicyScope
  case object All extends PolicyScope
}

/** Narrows a policy rule to only the datasets that carry at least one field
  * (recursively, through nested struct `properties`) tagged `sensitivityTag`
  * — e.g. "only datasets containing a `pii`-tagged field must be
  * catalog-registered." Absent from a `PolicyRule` (the common case), the
  * rule applies to every dataset in its `scope` regardless of tagging.
  */
case class PolicyCondition(sensitivityTag: String)

/** One entry in the org-owned catalogue of known `CONTROL`-declared datasets
  * (`OrgPolicy.controlTables`) — the trust boundary that makes `type:
  * CONTROL` something a contract author has to get *approved*, not merely
  * self-declare, the same way a `PolicyExemption` can only be granted by
  * whoever owns the policy document, never the contract's own author. This
  * list exists independently of whether anything actually checks it — an
  * organization can maintain it purely as a browsable catalogue of every
  * known control table, and only start enforcing it once a
  * `require_control_registration` policy rule is attached (see
  * `PolicyType.RequireControlRegistration`'s own doc for the opt-in/strict
  * split this affords).
  *
  * @param location the physical location a `CONTROL`-declared dataset must
  *   match to be considered registered — compared via
  *   `CrossContractValidator.normalize`/`sameLocation`, the identical
  *   location-matching this module already uses elsewhere, so a
  *   Windows-authored path's backslashes don't cause a false mismatch.
  * @param owner required, unlike every other field here beyond `location`:
  *   a catalogue entry with no accountable owner defeats the point of
  *   registering it at all.
  * @param purpose optional free-form explanation of what this control table
  *   is for — purely documentary, the catalogue's own equivalent of
  *   `Dataset.description`.
  * @param requiredFields the schema shape this control table is expected to
  *   carry (e.g. `watermark_ts`, `run_id`) — checked against the governed
  *   contract's own top-level schema fields for the matching dataset
  *   (`Schema.field`'s own lookup semantics, not recursive into nested
  *   structs, the same scope `RequireField` already uses). Empty means no
  *   shape is pinned — registration alone is enough to satisfy
  *   `require_control_registration` for this entry.
  * @param reviewBy optional expiry, identical semantics to
  *   `PolicyExemption.reviewBy`: a registration with no `reviewBy` never
  *   lapses; one whose `reviewBy` has passed stops counting as registered
  *   at all (see `isActive`) — the dataset falls back to being treated as
  *   entirely unregistered, the same way an expired exemption's suppressed
  *   violation re-surfaces, so a real control table has to be periodically
  *   re-approved rather than registered once and forgotten forever.
  */
case class ControlTableRegistration(
  location: String,
  owner: String,
  purpose: Option[String] = None,
  requiredFields: List[String] = Nil,
  reviewBy: Option[LocalDate] = None
) {

  /** Whether this registration still counts as active as of `now` — `true`
    * both when there's no expiry at all and when `now` is on-or-before it,
    * the identical boundary `PolicyExemption.covers` already uses.
    */
  def isActive(now: LocalDate): Boolean = reviewBy.forall(!now.isAfter(_))
}

/** Policy types Invaract itself knows how to interpret during org-policy
  * evaluation (see `InterpretedPolicy`, and `OrgPolicyEvaluator`) —
  * deliberately a narrow, closed set (mirroring `RuleType`'s own role for
  * contract-level rules), not a general policy-expression language. A
  * `PolicyRule.ruleType` outside this set is not necessarily inert, though:
  * if `OrgPolicy.customPolicyTypes` names a `CustomPolicyEvaluator`
  * implementation for it, `OrgPolicyEvaluator` dispatches to that instead —
  * see that field's own doc, and `CustomPolicyEvaluator`, for the
  * plug-in-without-a-source-change escape hatch this closed set doesn't
  * have to grow to cover every organization's own policy vocabulary. A
  * `ruleType` matching neither this set nor `customPolicyTypes` is still
  * recorded on `OrgPolicy.policies` but never acted on.
  */
object PolicyType {

  /** A dataset must declare `catalog.required: true` — optionally pinning
    * `technology` (e.g. `"hive"`) — so it's registered somewhere a catalog
    * consumer can discover it, not merely written to a physical location.
    */
  val RequireCatalog = "require_catalog"

  /** A dataset's schema must declare a field named `name` — optionally
    * pinning its `type` — e.g. a governance column every output must carry.
    * Only checked against top-level fields (`Schema.field`'s own lookup
    * semantics), not recursively into nested structs.
    */
  val RequireField = "require_field"

  /** Every field name in a dataset's schema — recursing into nested struct
    * `properties` — must fully match the regular expression `pattern`.
    */
  val FieldNamingConvention = "field_naming_convention"

  /** The contract itself (not any one dataset) must declare `key` in
    * `extensions` — optionally pinning its `value` — e.g. every contract
    * must carry `extensions: { owner: ... }`. Unlike the three types
    * above, this checks the contract as a whole exactly once, not once
    * per dataset in scope — `PolicyRule.scope`/`.when` have no effect on
    * it (see `InterpretedPolicy.ContractPolicy`).
    */
  val RequireExtension = "require_extension"

  /** A dataset must declare `format` as one of `formats` — e.g. only
    * `delta`/`iceberg`, never raw `parquet`/`csv` in production outputs.
    * Checked case-insensitively against `Dataset.format`, which must
    * itself be present (an output with no declared format at all doesn't
    * satisfy this, the same as any other required-but-absent check in
    * this file).
    */
  val RequireFormat = "require_format"

  /** A dataset must declare a non-blank `description` (see `Dataset`'s own
    * doc) — e.g. every output must say what it is, for catalog/data-
    * dictionary discoverability. No properties of its own: unlike every
    * other type in this object, there's nothing to configure beyond "this
    * dataset must have one," so `interpret` never fails for malformed
    * properties the way the others can.
    */
  val RequireDatasetDescription = "require_dataset_description"

  /** A conditional counterpart to `require_extension`: only when the
    * contract's `extensions` already satisfies `ifKey` (optionally pinned
    * to `ifValue`) must it *also* declare `thenKey` (optionally pinned to
    * `thenValue`) — e.g. "if `status` is `deprecated`, a `sunsetDate` must
    * be declared too." A contract that doesn't satisfy the `if` condition
    * at all is unaffected by this rule entirely, not merely exempted from
    * it. Like `require_extension`, this checks the contract as a whole
    * exactly once — `PolicyRule.scope`/`.when` have no effect on it (see
    * `InterpretedPolicy.ContractPolicy`).
    */
  val RequireExtensionIf = "require_extension_if"

  /** A dataset must declare a `type` (see `DatasetType`'s own doc:
    * `DATA_ASSET`/`SOURCE`/`CONTROL`) — optionally narrowed to a specific
    * allowed subset via `types` (e.g. "every output must be `DATA_ASSET`").
    * This is the mechanism that makes declaring `Dataset.datasetType`
    * mandatory or optional *per organization*, exactly as the spec asks:
    * `Dataset.datasetType` itself always defaults to `None` (purely
    * optional at the model level); an org that wants it required attaches
    * this policy type in `Enforce` mode, and an org that doesn't simply
    * never declares it — the same opt-in-by-policy shape
    * `RequireDatasetDescription`/`RequireCatalog` already establish for
    * their own optional `Dataset` fields.
    */
  val RequireDatasetType = "require_dataset_type"

  /** A dataset declared `CONTROL` (see `DatasetType`'s own doc) must not
    * carry any field whose `sensitivityTags` include one of `tags`
    * (optional; defaults to `PolicyRule.DefaultForbiddenControlTags` —
    * `pii`/`financial` — when omitted). A genuine control/watermark/
    * processing-calendar/reconciliation signal has no legitimate reason to
    * carry sensitive business data, so a `CONTROL`-declared dataset whose
    * schema does is a mechanical, single-contract signal that the
    * declaration may really be a relabeled `DATA_ASSET` avoiding
    * `DATA_ASSET`-scoped obligations (`require_catalog`, role-consistency
    * checking, etc.) — see docs/CONTRACT_MODEL.md's "Input and Output
    * Types" section. Deliberately narrower than `RequireDatasetType`: this
    * says nothing about whether a type is declared at all, only that a
    * dataset already declared `CONTROL` must not also carry these tags. A
    * dataset with no declared type, or one declared `DATA_ASSET`/`SOURCE`,
    * is never in scope for this check regardless of its own tags — that's
    * `RequireDatasetType`'s and ordinary sensitivity governance's own job,
    * not this one's.
    */
  val ForbidControlSensitivityTags = "forbid_control_sensitivity_tags"

  /** A dataset declared `CONTROL` must match a `location` entry in the
    * org-owned `OrgPolicy.controlTables` catalogue (see
    * `ControlTableRegistration`'s own doc) — and, when it does, its schema
    * must carry every field that entry's `requiredFields` names. An entry
    * whose `reviewBy` has lapsed (`ControlTableRegistration.isActive`
    * returns `false`) no longer counts as a match, so a `CONTROL` dataset
    * behind a stale registration is treated as unregistered, not silently
    * grandfathered in.
    *
    * @see optional `requireRegistration` property (default `true`) — the
    *   knob that makes this either the strict "CONTROL is the only thing
    *   allowed, and only once registered" gate, or the looser "registration
    *   is optional, but a dataset that *is* registered must still conform
    *   to what was approved" mode a platform rolling this out gradually may
    *   want instead:
    *   - `true` (default): an unregistered `CONTROL` dataset is itself a
    *     violation, in addition to a registered-but-nonconforming one.
    *   - `false`: an unregistered `CONTROL` dataset produces no violation at
    *     all — only a dataset that *does* match a `controlTables` entry is
    *     checked, against that entry's own `requiredFields`. This lets an
    *     organization build the catalogue up gradually (registering real
    *     control tables as they're found) without retroactively blocking
    *     every `CONTROL` declaration nobody's registered yet.
    *
    * Like every other `DatasetPolicy` here, `scope`/`when` still narrow
    * which datasets are considered (e.g. `scope: outputs` to only police
    * `CONTROL` outputs); an exemption still suppresses a violation from
    * this type the same as any other. Unlike every other built-in type,
    * though, evaluating this one needs data beyond the rule's own
    * `properties` — the whole `OrgPolicy.controlTables` catalogue — so
    * `OrgPolicyEvaluator.evaluateRule` special-cases this `ruleType` ahead
    * of the ordinary `CustomPolicyEvaluator`/`builtinEvaluators` dispatch
    * every other built-in type goes through; see that method's own doc.
    */
  val RequireControlRegistration = "require_control_registration"

  val All: Set[String] = Set(
    RequireCatalog,
    RequireField,
    FieldNamingConvention,
    RequireExtension,
    RequireFormat,
    RequireDatasetDescription,
    RequireExtensionIf,
    RequireDatasetType,
    ForbidControlSensitivityTags,
    RequireControlRegistration
  )

  /** The subset of `All` whose `InterpretedPolicy` is a `ContractPolicy`
    * rather than a `DatasetPolicy` — i.e. `scope`/`when` are inert on a
    * rule of this type. `OrgPolicyValidator` uses this to warn when either
    * is set on one, without hardcoding a per-type check that would need a
    * new branch for every future contract-level type.
    */
  val ContractLevelTypes: Set[String] = Set(RequireExtension, RequireExtensionIf)
}

/** A `PolicyRule`, decoded into one of the shapes Invaract currently knows
  * how to evaluate. Deliberately narrow, mirroring `PolicyType`'s members —
  * not a general policy-expression language.
  *
  * Split into two sub-traits by what a rule is checked *against* —
  * `OrgPolicyEvaluator.evaluateRule` dispatches on this split directly,
  * rather than a per-case-class special case, so a future contract-level
  * type (e.g. requiring something of `Contract.status`) has a natural,
  * already-exhaustive-checked home alongside `RequireExtension`:
  *
  *   - `DatasetPolicy` — checked once per dataset in `PolicyRule.scope`
  *     that also matches `.when`, if set.
  *   - `ContractPolicy` — checked once against the contract as a whole;
  *     `scope`/`when` don't apply (there's no per-dataset dimension to
  *     narrow).
  */
sealed trait InterpretedPolicy
object InterpretedPolicy {
  sealed trait DatasetPolicy extends InterpretedPolicy
  sealed trait ContractPolicy extends InterpretedPolicy

  case class RequireCatalog(technology: Option[String]) extends DatasetPolicy
  case class RequireField(name: String, fieldType: Option[String]) extends DatasetPolicy
  case class FieldNamingConvention(pattern: String) extends DatasetPolicy

  /** @param value if set, `extensions(key)` must equal this exact string
    *   (compared via `String.valueOf`, case-sensitive — extensions values
    *   are free-form, unlike `RequireField.fieldType`'s closed type-name
    *   vocabulary, so no case-folding is applied); if unset, any non-null
    *   value for `key` satisfies the rule.
    */
  case class RequireExtension(key: String, value: Option[String]) extends ContractPolicy

  /** @param formats at least one allowed format name (e.g. `"delta"`),
    *   matched case-insensitively against `Dataset.format`.
    */
  case class RequireFormat(formats: List[String]) extends DatasetPolicy

  /** No properties: `Dataset.description` must simply be present and
    * non-blank. A case object, not a case class, since there's nothing to
    * configure.
    */
  case object RequireDatasetDescription extends DatasetPolicy

  /** @param allowedTypes if set, `Dataset.datasetType` must be one of these
    *   specific types (e.g. "every output must be DATA_ASSET"); if unset,
    *   any of the three `DatasetType`s satisfies this rule - only the
    *   *presence* of a declared type is required, not a specific one.
    */
  case class RequireDatasetType(allowedTypes: Option[List[DatasetType]]) extends DatasetPolicy

  /** @param tags the lower-cased sensitivity tags forbidden on a
    *   `CONTROL`-declared dataset's schema — always resolved by
    *   `PolicyRule.interpret` (defaulted from
    *   `PolicyRule.DefaultForbiddenControlTags` when the rule's own `tags`
    *   property is absent), never empty: an explicit empty list is
    *   malformed, the same "empty means malformed" treatment
    *   `RequireFormat.formats` already gets.
    */
  case class ForbidControlSensitivityTags(tags: Set[String]) extends DatasetPolicy

  /** @param requireRegistration decoded from the rule's own optional
    *   `requireRegistration` property (default `true` when absent) — see
    *   `PolicyType.RequireControlRegistration`'s own doc for what `true`
    *   vs. `false` means. This is the only thing `PolicyRule.interpret`
    *   decodes for this type: the actual `OrgPolicy.controlTables` catalogue
    *   this flag is checked against isn't part of any one rule's own
    *   `properties`, so it can't live in this case class — see
    *   `OrgPolicyEvaluator.evaluateRule`'s special-case dispatch for this
    *   `ruleType`.
    */
  case class RequireControlRegistration(requireRegistration: Boolean) extends DatasetPolicy

  /** @param ifKey/ifValue the condition: `extensions(ifKey)` must be
    *   present (and, if `ifValue` is set, equal to it) for `thenKey`/
    *   `thenValue` to be checked at all. A contract not satisfying this
    *   condition produces no violation, the same as one this rule simply
    *   doesn't apply to.
    * @param thenKey/thenValue checked exactly like `RequireExtension`'s own
    *   `key`/`value`, only once the `if` condition holds.
    */
  case class RequireExtensionIf(ifKey: String, ifValue: Option[String], thenKey: String, thenValue: Option[String])
      extends ContractPolicy
}

/** One organizational policy rule. `id` is required and must be unique
  * within a document (`OrgPolicyValidator` errors on a duplicate) — it is
  * how `PolicyExemption`s target a rule and how a violation message
  * attributes itself to a specific, named policy rather than reading like a
  * generic contract error.
  *
  * @param properties rule-type-specific properties (e.g. `technology` for
  *   `require_catalog`, `name`/`type` for `require_field`, `pattern` for
  *   `field_naming_convention`) — the same open `Map[String, Any]` shape
  *   `ContractRule.properties` uses.
  * @param scope which of a contract's datasets this rule applies to.
  * @param when optionally narrows `scope` further by sensitivity tag.
  * @param mode `Enforce` (default) blocks; `Warn` only reports.
  */
case class PolicyRule(
  id: String,
  ruleType: String,
  properties: Map[String, Any],
  description: Option[String] = None,
  scope: PolicyScope = PolicyScope.All,
  when: Option[PolicyCondition] = None,
  mode: PolicyMode = PolicyMode.Enforce
) {

  /** Decodes `properties` into one of `InterpretedPolicy`'s shapes when
    * `ruleType` is one Invaract currently interprets and its properties are
    * well-formed. `None` covers both "not a policy type Invaract
    * interprets" and "malformed properties for one it does" — this stays
    * total/safe so evaluation never needs to catch an exception just to
    * check whether a rule applies; `OrgPolicyValidator` is where a
    * malformed *known* policy type becomes a reported issue instead, the
    * same split `ContractRule.interpret`/`ContractValidator` use.
    */
  def interpret: Option[InterpretedPolicy] = ruleType match {
    case PolicyType.RequireCatalog =>
      Some(InterpretedPolicy.RequireCatalog(properties.get("technology").map(String.valueOf)))
    case PolicyType.RequireField =>
      // Deliberately "fieldType", not "type": a policy rule's own YAML
      // mapping already has a top-level 'type' key naming *this* ruleType
      // ("require_field") - a same-named sub-property would collide with
      // it at the same mapping level. SnakeYAML doesn't reject a document
      // with two 'type:' keys; it silently keeps only the *last* one,
      // which would overwrite the intended ruleType itself (corrupting it
      // into whatever field type was named) rather than raising anything -
      // confirmed empirically, not assumed, the same "Norway problem"-style
      // caution ContractRule.interpret's own 'columns', not 'on', decision
      // documents for an unrelated but structurally identical YAML pitfall.
      properties.get("name").map(String.valueOf).map { name =>
        InterpretedPolicy.RequireField(name, properties.get("fieldType").map(String.valueOf))
      }
    case PolicyType.FieldNamingConvention =>
      properties.get("pattern").map(String.valueOf).filter(PolicyRule.isValidRegex).map(InterpretedPolicy.FieldNamingConvention)
    case PolicyType.RequireExtension =>
      properties.get("key").map(String.valueOf).map { key =>
        InterpretedPolicy.RequireExtension(key, properties.get("value").map(String.valueOf))
      }
    case PolicyType.RequireFormat =>
      PolicyRule.parseStringList(properties.get("formats")).filter(_.nonEmpty).map(InterpretedPolicy.RequireFormat)
    case PolicyType.RequireDatasetDescription =>
      Some(InterpretedPolicy.RequireDatasetDescription)
    case PolicyType.RequireDatasetType =>
      properties.get("types") match {
        case None => Some(InterpretedPolicy.RequireDatasetType(None))
        case Some(raw) =>
          // Reuses RequireFormat's own list-vs-scalar coercion
          // (parseStringList takes a raw property value, not the "formats"
          // key name itself) rather than duplicating it - the shape ("a
          // YAML list, or a bare scalar as shorthand for a one-element
          // list") is identical.
          PolicyRule.parseStringList(Some(raw)).filter(_.nonEmpty).flatMap { rawTypes =>
            val parsed = rawTypes.map(DatasetType.parse)
            // All-or-nothing: one unrecognized type name in the list makes
            // the whole property malformed, the same "None covers malformed
            // properties too" total/safe convention every other type here
            // follows - OrgPolicyValidator is where that becomes a reported
            // issue, not a partially-applied list silently dropping the bad
            // entry.
            if (parsed.forall(_.isDefined)) Some(InterpretedPolicy.RequireDatasetType(Some(parsed.flatten)))
            else None
          }
      }
    case PolicyType.ForbidControlSensitivityTags =>
      properties.get("tags") match {
        case None => Some(InterpretedPolicy.ForbidControlSensitivityTags(PolicyRule.DefaultForbiddenControlTags))
        case some =>
          PolicyRule.parseStringList(some).filter(_.nonEmpty).map { rawTags =>
            InterpretedPolicy.ForbidControlSensitivityTags(rawTags.map(_.toLowerCase).toSet)
          }
      }
    case PolicyType.RequireControlRegistration =>
      properties.get("requireRegistration") match {
        case None                       => Some(InterpretedPolicy.RequireControlRegistration(requireRegistration = true))
        case Some(b: java.lang.Boolean) => Some(InterpretedPolicy.RequireControlRegistration(b.booleanValue))
        case Some(_)                    => None // malformed - not a boolean
      }
    case PolicyType.RequireExtensionIf =>
      for {
        ifKey <- properties.get("ifKey").map(String.valueOf)
        thenKey <- properties.get("thenKey").map(String.valueOf)
      } yield InterpretedPolicy.RequireExtensionIf(
        ifKey,
        properties.get("ifValue").map(String.valueOf),
        thenKey,
        properties.get("thenValue").map(String.valueOf)
      )
    case _ => None
  }
}

object PolicyRule {
  private[contract] def isValidRegex(pattern: String): Boolean =
    try {
      java.util.regex.Pattern.compile(pattern)
      true
    } catch {
      case _: java.util.regex.PatternSyntaxException => false
    }

  /** Coerces a `PolicyRule` property that may be either a YAML list
    * (`formats: [delta, iceberg]`, decoded by SnakeYAML as a
    * `java.util.List`) or a bare scalar (`formats: delta`) as shorthand for
    * a single-element list — used by `require_format`'s `formats`,
    * `require_dataset_type`'s `types`, and `forbid_control_sensitivity_tags`'s
    * `tags`, the only properties in this file that ever need list coercion
    * (every other property is a single scalar). `None` when the property is
    * absent; an empty list is possible (e.g. `formats: []`) and is filtered
    * out by the caller, the same "empty means malformed" treatment
    * `pattern`/`name` already get.
    */
  private[contract] def parseStringList(raw: Option[Any]): Option[List[String]] = raw match {
    case Some(list: java.util.List[_]) => Some(list.asScala.toList.map(String.valueOf).filter(_.nonEmpty))
    case Some(list: Seq[_])            => Some(list.toList.map(String.valueOf).filter(_.nonEmpty))
    case Some(scalar)                  => Some(List(String.valueOf(scalar)).filter(_.nonEmpty))
    case None                          => None
  }

  /** `forbid_control_sensitivity_tags`'s own default `tags` set, used
    * whenever a rule of that type omits the property entirely — see
    * `PolicyType.ForbidControlSensitivityTags`'s own doc for why these two.
    */
  private[contract] val DefaultForbiddenControlTags: Set[String] = Set("pii", "financial")
}

/** Extension point for an organizational policy type Invaract's own
  * built-in `PolicyType` set doesn't cover — resolved reflectively via
  * `OrgPolicy.customPolicyTypes` (`CustomPolicyEvaluatorFactory`), the same
  * "class name named in config, public no-arg constructor, loaded once"
  * mechanism `spark-adapter`'s `NotificationSinkFactory` already
  * established for `NotificationSink`. See docs/CONTRACT_MODEL.md's
  * "Custom policy types" section.
  *
  * Unlike a built-in `InterpretedPolicy`, there is no `DatasetPolicy`/
  * `ContractPolicy` split here: an implementation receives the whole
  * `Contract` and the raw `PolicyRule` (`scope`/`when`/`mode`/`properties`
  * all included) and decides for itself what to check and how — or
  * whether — to honor `rule.scope`/`rule.when`, the same freedom
  * `PolicyRule.properties` already gives every built-in type's own
  * `interpret`.
  *
  * `OrgPolicyEvaluator.evaluate` already applies `PolicyExemption`
  * coverage and `PolicyMode` splitting uniformly, *before* dispatching to
  * a custom type (see `evaluateRule`) — an implementation gets both for
  * free and never needs to reimplement either. `rule.mode` is available on
  * every returned `PolicyViolation` regardless of what an implementation
  * sets there; only `OrgPolicyEvaluator`'s own `evaluate` decides which of
  * `enforceViolations`/`warnViolations` a violation ends up in, from
  * `rule.mode` — an implementation's own `PolicyViolation.mode` field is
  * informational only, not consulted for that split.
  *
  * Required to be stateless: `CustomPolicyEvaluatorFactory` constructs one
  * instance per class name and reuses it across every rule/contract that
  * names it, the same assumption `NotificationSink` makes about a sink
  * instance surviving many `publish` calls.
  */
trait CustomPolicyEvaluator {
  def evaluate(contract: Contract, rule: PolicyRule): List[PolicyViolation]
}

/** A platform-controlled exception to one or more policy rules, for one
  * specific contract. Deliberately lives in the *policy* document, not the
  * contract: a contract's own author cannot exempt their own contract from
  * an org-wide rule, since that would defeat the rule's purpose — only
  * whoever owns this policy file can.
  *
  * @param reviewBy optional expiry. An exemption with no `reviewBy` never
  *   expires; one with a past `reviewBy` stops applying — the violation it
  *   was suppressing re-surfaces — rather than remaining a permanent,
  *   silent bypass. `OrgPolicyValidator` warns (does not error) when this
  *   has already passed, so an expired exemption is visible without being
  *   fatal.
  */
case class PolicyExemption(
  contractId: String,
  policyIds: List[String],
  reason: String,
  reviewBy: Option[LocalDate] = None
) {

  /** Whether this exemption currently covers `policyId` for `contractId0`,
    * as of `now`. `reviewBy.forall(!now.isAfter(_))` is `true` both when
    * there's no expiry at all and when `now` is on-or-before it — only a
    * `now` strictly after `reviewBy` makes the exemption inactive.
    */
  def covers(contractId0: String, policyId: String, now: LocalDate): Boolean =
    contractId0 == contractId && policyIds.contains(policyId) && reviewBy.forall(!now.isAfter(_))
}

/** What this policy contributes to every contract it governs, merged in
  * before verification runs — reusing `RuleVerifier`/`StructuralVerifier`
  * wholesale instead of needing a parallel evaluator for behavioral checks.
  *
  * @param rules `ContractRule`s merged into a governed contract's own
  *   `rules` (see `OrgPolicyEvaluator.applyInjectedRules`) — e.g. an org-wide
  *   `forbid_unconditional_delete` no contract author needs to remember to
  *   declare themselves.
  * @param minVerificationOptions floors ORed onto a job's `VerificationOptions`
  *   by `spark-adapter`'s `ContractEnforcementRule` (e.g.
  *   `"rejectUndeclaredFields" -> true`) — a flag a job's own code left
  *   `false` can still be forced `true` by policy; never the reverse. Keyed
  *   by option name rather than typed here, since `VerificationOptions`
  *   itself is a `spark-adapter` type this Spark-independent module cannot
  *   depend on.
  */
case class InjectedDefaults(rules: List[ContractRule] = Nil, minVerificationOptions: Map[String, Boolean] = Map.empty)

/** What "stronger semantic and guarantee validation" (docs/CONTRACT_MODEL.md's
  * "Input and Output Types" section, "Type guarantee checks") this policy
  * turns on for `TypeGuaranteeValidator.evaluate` — the mechanism that makes
  * *which* guarantee checks run, and whether a `Contradicts` verdict blocks a
  * lint/pipeline run at all, an organizational choice rather than a hardcoded
  * one, the same "mandatory or optional, per organization" shape
  * `require_dataset_type`/`roleConsistency` already establish elsewhere in
  * this feature.
  *
  * @param enabled `TypeGuaranteeType` names (built-in or from
  *   `customTypeGuaranteeTypes`) to actually run — empty (the default) means
  *   none run at all, the same opt-in-by-default `VerificationOptions`
  *   itself uses for every one of its own flags. An entry naming neither a
  *   built-in type nor a `customTypeGuaranteeTypes` key is recorded but
  *   never evaluated (`OrgPolicyValidator` warns on this, the same
  *   "will never be evaluated" treatment an unrecognized `policies[].type`
  *   gets).
  * @param mode `Enforce` (default) makes a `Contradicts` verdict from any
  *   enabled check block a caller (`CrossContractLintCli`, a future
  *   registry-side consumer); `Warn` reports every verdict without ever
  *   blocking — the same rollout mechanism `PolicyMode` already gives every
  *   other policy type. One mode for the whole block, not per-check: an org
  *   wanting a stricter mode for one specific check simply enables only
  *   that one in its own layer/policy, the same composition
  *   `OrgPolicyEvaluator.evaluateLayers` already gives every other policy
  *   type.
  * @param customTypeGuaranteeTypes maps a `TypeGuaranteeType` name this
  *   document's `enabled` list uses to the fully-qualified class name of a
  *   `TypeGuaranteeCheck` implementation that performs it — the
  *   plug-in-without-editing-Invaract's-own-source extension point for a
  *   guarantee check the built-in set doesn't cover, the identical
  *   reflective mechanism `customPolicyTypes`/`Contract.customRuleTypes`
  *   already establish. A name also present in `TypeGuaranteeType.All` is
  *   inert here — the built-in check always wins (`OrgPolicyValidator`
  *   warns on this). Resolved by `TypeGuaranteeCheckFactory`.
  */
case class TypeGuaranteeConfig(
  enabled: List[String] = Nil,
  mode: PolicyMode = PolicyMode.Enforce,
  customTypeGuaranteeTypes: Map[String, String] = Map.empty
)

/** The root organizational policy document: a platform-owned artifact,
  * independent of any one contract, expressing rules that apply across
  * every contract in an organization. Attached to a Spark job via the
  * `spark.invaract.orgPolicy` conf key (`spark-adapter`'s
  * `ContractEnforcementRule`) — see docs/CONTRACT_MODEL.md's
  * "Organizational Policy" section.
  *
  * @param customPolicyTypes maps a `PolicyRule.ruleType` this document uses
  *   to the fully-qualified class name of a `CustomPolicyEvaluator`
  *   implementation that evaluates it — the plug-in-without-editing-
  *   Invaract's-own-source extension point for an organizational policy
  *   type the built-in `PolicyType` set doesn't cover. A `ruleType` also
  *   present in `PolicyType.All` is inert here — the built-in
  *   interpretation always wins (`OrgPolicyValidator` warns on this).
  *   Resolved by `CustomPolicyEvaluatorFactory`; see
  *   docs/CONTRACT_MODEL.md's "Custom policy types" section.
  * @param typeGuarantees which "stronger semantic and guarantee validation"
  *   checks (`TypeGuaranteeValidator`) this document turns on across the
  *   contracts it governs, and how a `Contradicts` verdict is handled — see
  *   `TypeGuaranteeConfig`'s own doc.
  * @param controlTables the org-owned catalogue of known `CONTROL`-declared
  *   datasets — see `ControlTableRegistration`'s own doc. Exists
  *   independently of `policies`: an organization can maintain this purely
  *   as a browsable catalogue with no enforcement at all, and only start
  *   checking it once a `require_control_registration` policy rule
  *   (`PolicyType.RequireControlRegistration`) is attached. Appended last
  *   (like `typeGuarantees` before it) specifically to keep this addition
  *   binary-compatible with existing compiled callers — see the API
  *   Compatibility Requirement's own worked example for why a new case-class
  *   field belongs at the end, not the middle.
  */
case class OrgPolicy(
  version: String,
  policies: List[PolicyRule] = Nil,
  inject: InjectedDefaults = InjectedDefaults(),
  exemptions: List[PolicyExemption] = Nil,
  customPolicyTypes: Map[String, String] = Map.empty,
  typeGuarantees: TypeGuaranteeConfig = TypeGuaranteeConfig(),
  controlTables: List[ControlTableRegistration] = Nil
)
