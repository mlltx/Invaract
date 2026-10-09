// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.verification

import com.invaract.contract.Contract
import com.invaract.verification.notification.{NotificationConfig, NotificationSink, NotificationSinkFactory}

import org.slf4j.LoggerFactory

import scala.util.control.NonFatal

/** What a job is attached to, decided from configuration alone. */
sealed trait AttachPlan {

  /** Where validation events go, if a sink is configured. */
  def sink: Option[NotificationSink]
}

object AttachPlan {

  /** Enforce `contract`: a write that violates it is blocked. */
  final case class Enforce(contract: Contract, sink: Option[NotificationSink]) extends AttachPlan

  /** Dry-run: infer a contract from what the job really does, enforce nothing. */
  final case class DryRun(sink: Option[NotificationSink]) extends AttachPlan
}

/** The decisions every adapter makes once, when it is attached to a job whose
  * source it does not own: dry-run or enforcement, which contract (a path or a
  * `registry://` reference), and which notification sink. They depend only on
  * configuration, so they live here and not in an adapter - an adapter supplies
  * a `ConfigSource` that spells the neutral `InvaractConf` names its engine's
  * way, calls `select`, and acts on the `AttachPlan` it gets back with its
  * engine's own hook (CLAUDE.md's External Attachability Requirement;
  * docs/MULTI_ENGINE_ADAPTERS.md).
  *
  * `select` only decides; installing a check rule, listener or runner stays the
  * adapter's. Location resolution, option overlay and organizational policy are
  * `VerificationSetup`'s, applied to the contract a plan carries.
  */
object AttachSetup {
  private val logger = LoggerFactory.getLogger(AttachSetup.getClass)

  /** The plan `config` describes.
    *
    * Dry-run (`dryRun=true`) ignores `contract` entirely and is lenient about its
    * sink, because its whole promise is that it is safe to attach to a job
    * nobody here owns: a sink that cannot be set up costs one WARN and a plan
    * with no sink, never the job. Enforcement requires `contract`, and a sink
    * that cannot be set up fails the job at start-up - a typo'd sink class is
    * caught the moment the job starts, not after it has produced no events.
    */
  def select(config: ConfigSource): AttachPlan =
    if (config.get(InvaractConf.DryRun).exists(_.toBoolean)) {
      AttachPlan.DryRun(lenientSink(config))
    } else {
      val reference = config.get(InvaractConf.Contract).getOrElse(
        throw new IllegalStateException(
          s"A contract is required: set '${config.describe(InvaractConf.Contract)}' " +
            s"(or '${config.describe(InvaractConf.DryRun)}=true' for dry-run mode, which needs no contract at all)."
        )
      )
      AttachPlan.Enforce(ContractReference.resolve(reference, config), strictSink(config))
    }

  /** The sink `notifyConfig` names, failing loudly if it cannot be built. */
  def strictSink(config: ConfigSource): Option[NotificationSink] =
    config.get(InvaractConf.NotifyConfig).flatMap(path => NotificationSinkFactory.create(NotificationConfig.load(path)))

  /** The sink `notifyConfig` names, or `None` (after one WARN) if it cannot be built. */
  def lenientSink(config: ConfigSource): Option[NotificationSink] =
    config.get(InvaractConf.NotifyConfig).flatMap { path =>
      try NotificationSinkFactory.create(NotificationConfig.load(path))
      catch {
        case NonFatal(e) =>
          logger.warn(
            s"Invaract dry-run mode: could not set up the notification sink named by " +
              s"'${config.describe(InvaractConf.NotifyConfig)}=$path' ($e); falling back to logging inferred contracts. " +
              "The job is not affected."
          )
          None
      }
    }
}
