// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.beamadapter

/** What the translator (`BeamTranslator`) needs to know about the steps of a pipeline: which ones read or write data, which make
  * data in the job itself, which combine collections without matching rows, and which are schema transforms. Everything else
  * the translator does - the walk, how an unknown step keeps the reads beneath it visible, what fails closed, which steps only
  * report on writes - is the same however the pipeline was described, so it is written once.
  *
  * There is one catalogue per way a pipeline reaches the adapter: `JavaSteps` for a Java pipeline constructed in this JVM
  * (`BeamGraph`), which knows Beam's Java IO classes, and `PythonSteps` for a pipeline read from its portable form
  * (`PortablePipeline`), which knows the Python SDK's (docs/BEAM_PYTHON_ADAPTER.md).
  */
private[beamadapter] trait StepCatalogue {

  /** The read `step` is, if it is one. Composites are asked before what they contain, so the outermost recognised step wins. */
  def read(step: BeamStep): Option[IoRead]

  /** The write `step` is, if it is one (other than a Java file write, which `isFileWrite` and `fileWrite` describe). */
  def write(step: BeamStep): Option[IoWrite]

  /** Whether `step` is the primitive a Java file write is built on (`WriteFiles`); the outermost step holding exactly one is the write. */
  def isFileWrite(step: BeamStep): Boolean

  /** The file write a step holding exactly one `isFileWrite` step makes. */
  def fileWrite(unit: BeamStep): IoWrite

  /** Whether `step` makes data in the job itself (a `Create`): never a read from outside the pipeline, whatever it is built from. */
  def isInJobData(step: BeamStep): Boolean

  /** Whether `step` concatenates collections without matching rows of one with another (`Flatten`). */
  def isUnion(step: BeamStep): Boolean

  /** Whether `step` turns lines of text into rows whose columns are known (`JsonToRow`). */
  def isTextParser(step: BeamStep): Boolean

  /** The format a text write of `step`'s output is in, when `step` turns rows into lines of a known format (`ToJson`). */
  def textFormatterFormat(step: BeamStep): Option[String]

  /** Whether `step` is a schema transform whose columns can be read (`BeamShapes`), without reading them. */
  def recognisesShape(step: BeamStep): Boolean

  /** What `step` does with columns, when it is such a schema transform. */
  def shape(step: BeamStep): Option[SchemaShape]
}

/** The steps of a Java pipeline: Beam's Java IO (`BeamIO`) and schema transforms (`BeamShapes`). */
private[beamadapter] object JavaSteps extends StepCatalogue {
  override def read(step: BeamStep): Option[IoRead] = BeamIO.read(step)
  override def write(step: BeamStep): Option[IoWrite] = BeamIO.write(step)
  override def isFileWrite(step: BeamStep): Boolean = BeamIO.isFileWrite(step)
  override def fileWrite(unit: BeamStep): IoWrite = BeamIO.fileWrite(unit)
  override def isInJobData(step: BeamStep): Boolean = BeamIO.isInJobData(step)
  override def isUnion(step: BeamStep): Boolean = BeamIO.isUnion(step)
  override def isTextParser(step: BeamStep): Boolean = BeamIO.isTextParser(step)
  override def textFormatterFormat(step: BeamStep): Option[String] = BeamIO.textFormatterFormat(step)
  override def recognisesShape(step: BeamStep): Boolean = BeamShapes.recognises(step)
  override def shape(step: BeamStep): Option[SchemaShape] = BeamShapes.of(step)
}
