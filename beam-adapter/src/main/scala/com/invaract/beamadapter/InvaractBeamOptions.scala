// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.beamadapter

import com.invaract.verification.{ConfigSource, InvaractConf}
import org.apache.beam.sdk.options.{Description, PipelineOptions, PipelineOptionsRegistrar}

/** Invaract's settings as Beam pipeline options: `--invaract<Name>=value` on the job's command line, like any Beam option.
  *
  * This is how the adapter satisfies the External Attachability Requirement (CLAUDE.md): a platform team turns checking on
  * for a job whose source it does not own with `--runner=com.invaract.beamadapter.InvaractRunner --invaractContract=...`.
  * The options are found by Beam through `InvaractOptionsRegistrar` (a `ServiceLoader` entry), so the job needs no code
  * that knows about them. Every value is text, because the engine-neutral settings (`InvaractConf`) are; the flags are
  * parsed by the shared setup.
  */
trait InvaractBeamOptions extends PipelineOptions {
  @Description("The data contract to enforce: a file path, or registry://<id>@<version>.")
  def getInvaractContract: String
  def setInvaractContract(value: String): Unit

  @Description("true: observe and infer a contract from the job's writes instead of enforcing one; needs no contract.")
  def getInvaractDryRun: String
  def setInvaractDryRun(value: String): Unit

  @Description("Path of a notification-sink .properties file: where validation events are sent.")
  def getInvaractNotifyConfig: String
  def setInvaractNotifyConfig(value: String): Unit

  @Description("Base URL of the contract registry a registry:// contract is fetched from.")
  def getInvaractRegistryUrl: String
  def setInvaractRegistryUrl(value: String): Unit

  @Description("Class name of the registry client to load, overriding the default.")
  def getInvaractRegistryClientClass: String
  def setInvaractRegistryClientClass(value: String): Unit

  @Description("Path of an id=location .properties file that ref://<id> locations in the contract resolve against.")
  def getInvaractLocationMap: String
  def setInvaractLocationMap(value: String): Unit

  @Description("Path of an organizational policy document layered on the contract.")
  def getInvaractOrgPolicy: String
  def setInvaractOrgPolicy(value: String): Unit

  @Description("Comma-separated paths of further organizational policy layers.")
  def getInvaractOrgPolicyOverlays: String
  def setInvaractOrgPolicyOverlays(value: String): Unit

  @Description("true: a read of a dataset the contract does not declare is blocked.")
  def getInvaractRejectUndeclaredInputs: String
  def setInvaractRejectUndeclaredInputs(value: String): Unit

  @Description("true: an output column the contract does not declare is blocked.")
  def getInvaractRejectUndeclaredFields: String
  def setInvaractRejectUndeclaredFields(value: String): Unit

  @Description("true: compute a semantic fingerprint of the job (the Beam adapter declares this unsupported, so a contract run with it is refused).")
  def getInvaractComputeFingerprint: String
  def setInvaractComputeFingerprint(value: String): Unit

  @Description("true: statically prove the contract's value constraints (declared unsupported on Beam, so a run with it is refused).")
  def getInvaractStaticDataQuality: String
  def setInvaractStaticDataQuality(value: String): Unit

  @Description("true: check each input's declared role against how the job uses it (declared unsupported on Beam, so a run with it is refused).")
  def getInvaractRoleConsistency: String
  def setInvaractRoleConsistency(value: String): Unit

  @Description("Comma-separated names of steps the platform trusts not to move governed data (a metrics or a notification step), * for any run of characters: a step that ends the pipeline without a recognised write is not refused when named here.")
  def getInvaractTrustedSteps: String
  def setInvaractTrustedSteps(value: String): Unit

  @Description("The runner the checked pipeline is handed to once it passes (a runner's simple or full class name). Default: DirectRunner.")
  def getInvaractDelegate: String
  def setInvaractDelegate(value: String): Unit
}

/** The engine-neutral settings a Beam pipeline can be given: every one but `failOnMissingOutput`, which is for an adapter that finds a
  * missing output only once the job has ended, where Beam's adapters refuse the job before it starts. `BeamConfigSource` and the
  * Python package's flags (`PythonConfigSource`) are each checked against this list in tests, so a new setting cannot be left
  * unreachable on either. */
private[beamadapter] object BeamSettings {
  val Names: List[String] = InvaractConf.All.filterNot(_ == InvaractConf.FailOnMissingOutput)
}

/** The Beam spelling of the engine-neutral settings: `--invaract<Name>`, read off the pipeline's own options. */
private[beamadapter] final class BeamConfigSource(options: InvaractBeamOptions) extends ConfigSource {

  override def get(name: String): Option[String] =
    Option(name match {
      case InvaractConf.Contract               => options.getInvaractContract
      case InvaractConf.DryRun                 => options.getInvaractDryRun
      case InvaractConf.NotifyConfig           => options.getInvaractNotifyConfig
      case InvaractConf.RegistryUrl            => options.getInvaractRegistryUrl
      case InvaractConf.RegistryClientClass    => options.getInvaractRegistryClientClass
      case InvaractConf.LocationMap            => options.getInvaractLocationMap
      case InvaractConf.OrgPolicy              => options.getInvaractOrgPolicy
      case InvaractConf.OrgPolicyOverlays      => options.getInvaractOrgPolicyOverlays
      case InvaractConf.RejectUndeclaredInputs => options.getInvaractRejectUndeclaredInputs
      case InvaractConf.RejectUndeclaredFields => options.getInvaractRejectUndeclaredFields
      case InvaractConf.ComputeFingerprint     => options.getInvaractComputeFingerprint
      case InvaractConf.StaticDataQuality      => options.getInvaractStaticDataQuality
      case InvaractConf.RoleConsistency        => options.getInvaractRoleConsistency
      case InvaractConf.TrustedSteps           => options.getInvaractTrustedSteps
      case _                                   => null
    }).filter(_.trim.nonEmpty)

  override def describe(name: String): String = BeamConfigSource.spelling(name)
}

private[beamadapter] object BeamConfigSource {

  /** How a Java pipeline is given the setting `name`: `--invaract` and its name, capitalised (`--invaractDryRun`). */
  def spelling(name: String): String = "--invaract" + name.capitalize
}

/** Registers `InvaractBeamOptions` with Beam, found through `META-INF/services/org.apache.beam.sdk.options.PipelineOptionsRegistrar`. */
class InvaractOptionsRegistrar extends PipelineOptionsRegistrar {
  override def getPipelineOptions: java.lang.Iterable[Class[_ <: PipelineOptions]] =
    java.util.Collections.singletonList[Class[_ <: PipelineOptions]](classOf[InvaractBeamOptions])
}
