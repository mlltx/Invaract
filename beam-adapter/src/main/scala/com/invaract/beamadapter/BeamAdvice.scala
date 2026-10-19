// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.beamadapter

import com.invaract.verification.{InvaractConf, UnverifiableOperation}

/** What a refusal of something the Beam adapter cannot check (`UNVERIFIABLE_WRITE`) tells the job's owner to do, in the terms of
  * the SDK the pipeline was written with. The reasons and the advice are the same for the Java and Python SDKs; only the spelling
  * of a setting (`setting`) and the docs page that explains what can be told about a pipeline (`guide`, with the title of its
  * section on steps that end the pipeline, `endingSteps`) differ.
  */
private[beamadapter] final case class BeamAdvice(setting: String => String, guide: String, endingSteps: String) {

  private val whatCannotBeTold = s"""see "Formats, types and what cannot be told" in the docs' guide ($guide)"""

  /** A step that ends the pipeline without being a recognised write, which a platform may name as trusted. */
  def endingStep(name: String, problem: String): UnverifiableOperation =
    UnverifiableOperation(
      name,
      problem,
      s"If this step moves no data the contract governs (it logs, counts or notifies), a platform can name it as trusted with " +
        s"${setting(InvaractConf.TrustedSteps)}=$name. If it writes data, write through an IO transform Invaract recognises, with " +
        s"""its destination fixed when the pipeline is built - see "$endingSteps" in the docs' guide ($guide)."""
    )

  /** A write whose destination or sink cannot be told before the job runs, which naming as trusted would not let through. */
  def unplacedWrite(name: String, problem: String): UnverifiableOperation =
    UnverifiableOperation(
      name,
      problem,
      "Write through an IO transform Invaract recognises, with its destination fixed when the pipeline is built: a destination " +
        "the job computes while it runs, or a template parameter, is known only after the check. Naming the step as trusted does " +
        s"not let it through, since where it writes cannot be told - $whatCannotBeTold."
    )

  /** A recognised write that does not state what the contract asks of it. */
  def unstatedWrite(location: String, problem: String): UnverifiableOperation =
    UnverifiableOperation(
      s"the write to $location",
      problem,
      s"Make the pipeline state what the message says it does not, or leave that requirement out of this output in the contract - $whatCannotBeTold."
    )
}

private[beamadapter] object BeamAdvice {
  val Java: BeamAdvice = BeamAdvice(BeamConfigSource.spelling, "guides/using-invaract-with-apache-beam", "Steps that only report")
  val Python: BeamAdvice = BeamAdvice(PythonConfigSource.spelling, "guides/using-invaract-with-beam-python", "Steps that end the pipeline")
}
