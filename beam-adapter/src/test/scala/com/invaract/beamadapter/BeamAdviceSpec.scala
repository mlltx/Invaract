// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.beamadapter

import com.invaract.verification.{ContractViolationException, Violation, ViolationType}
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.{GenerateSequence, TextIO}
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.{Create, ParDo}
import org.scalatest.funsuite.AnyFunSuite

/** What a refusal of something the Beam adapters cannot check tells the job's owner (`BeamAdvice`): the advice for the kind of
  * refusal it is, in the terms of the SDK the pipeline was written with, and nothing about another engine. */
class BeamAdviceSpec extends AnyFunSuite {
  import PortableCheckSpec.{ReportContract, check, file, freshRoot, recording}

  private def refusal(p: Pipeline, contract: com.invaract.contract.Contract = ExampleJob.contract()): Violation = {
    val e = intercept[ContractViolationException](InvaractBeam.enforce(contract).verify(p))
    val Seq(v) = e.result.violations
    assert(v.violationType == ViolationType.UnverifiableWrite)
    assert(!v.remediation.contains("Spark") && !v.remediation.contains("FailClosedCommands"), v.remediation)
    v
  }

  test("Java: a step that ends the pipeline is named, and the advice is to trust it with --invaractTrustedSteps or write through an IO transform") {
    val p = Pipeline.create(PipelineOptionsFactory.create())
    p.apply("Counter", GenerateSequence.from(0).to(5)).apply("PushToSomeService", ParDo.of(new BeamScenarioJobs.SideEffectLong))
    val v = refusal(p)
    assert(v.actual.contains("PushToSomeService"))
    assert(v.message.startsWith("A step that ends the pipeline but is not a recognised write: PushToSomeService (ParDo"), v.message)
    assert(v.remediation.contains("--invaractTrustedSteps=PushToSomeService"))
    assert(v.remediation.contains(""""Steps that only report" in the docs' guide (guides/using-invaract-with-apache-beam)"""))
  }

  test("Java: a write whose destination is a template parameter is not one trusting would let through, and the advice says so") {
    val o = PipelineOptionsFactory.fromArgs().as(classOf[TemplateOptions])
    val p = Pipeline.create(o)
    p.apply("Lines", Create.of("a")).apply("WriteLines", TextIO.write().to(o.getOutputDir))
    val v = refusal(p)
    assert(v.actual.exists(_.startsWith("WriteLines")), v.actual)
    assert(v.message.startsWith("A write whose destination is a template parameter (outputDir)"))
    assert(v.remediation.contains("Naming the step as trusted does not let it through"))
    assert(!v.remediation.contains("--invaractTrustedSteps"))
    assert(v.remediation.contains(""""Formats, types and what cannot be told" in the docs' guide (guides/using-invaract-with-apache-beam)"""))
  }

  test("Java: a recognised write that does not state what the contract asks is named by its location, and the advice is to state it or ask less") {
    val p = Pipeline.create(PipelineOptionsFactory.create())
    p.apply("Lines", Create.of("a", "b")).apply("WriteLines", TextIO.write().to("/data/out/lines"))
    val v = refusal(p, ExampleJob.contract("out/lines"))
    assert(v.actual.exists(a => a.startsWith("the write to ") && a.endsWith("/data/out/lines")), v.actual)
    assert(v.message.contains("exposes no schema"))
    assert(v.remediation.startsWith("Make the pipeline state what the message says it does not, or leave that requirement out of this output"))
    assert(v.remediation.contains("guides/using-invaract-with-apache-beam"))
  }

  test("Python: the same advice, spelled as the Python package spells its flags, pointing at the Python guide") {
    val root = freshRoot()
    val contract = s"--invaract_contract=${file(root, "c.yaml", ReportContract)}"
    val (ending, endingOut) = check(s"--pipeline=${recording(root, "side_effects")}", contract)
    assert(ending == PortableCheck.Refused && endingOut.contains("[UNVERIFIABLE_WRITE]"), endingOut)
    assert(endingOut.contains("--invaract_trusted_steps=PushToService"))
    assert(endingOut.contains(""""Steps that end the pipeline" in the docs' guide (guides/using-invaract-with-beam-python)"""))
    val (unplaced, unplacedOut) = check(s"--pipeline=${recording(root, "template_job")}", contract)
    assert(unplaced == PortableCheck.Refused && unplacedOut.contains("Naming the step as trusted does not let it through"), unplacedOut)
    assert(unplacedOut.contains("guides/using-invaract-with-beam-python") && !unplacedOut.contains("--invaractTrustedSteps"))
    assert(!endingOut.contains("Spark") && !unplacedOut.contains("Spark"))
  }

  test("each SDK's advice names the setting as that SDK spells it") {
    assert(BeamAdvice.Java.endingStep("S", "p").remediation.contains("--invaractTrustedSteps=S"))
    assert(BeamAdvice.Python.endingStep("S", "p").remediation.contains("--invaract_trusted_steps=S"))
    assert(BeamAdvice.Java.unplacedWrite("W", "p").name == "W" && BeamAdvice.Java.unplacedWrite("W", "p").problem == "p")
    assert(BeamAdvice.Python.unstatedWrite("out/x", "p").name == "the write to out/x")
  }
}
