// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.verification

import com.invaract.contract.{Contract, CsvOptions, CsvText, Dataset}
import com.invaract.ir.{Plan, Read, Write}

/** `check.csvOptions`: each CSV option a dataset declares against the one its read or write used. Both sides must be known,
  * as for `format`: an option the dataset does not declare, or one the adapter does not report, is not compared, and an
  * adapter that reports no options at all (`CheckedWrite.formatOptions` empty) checks nothing. The comparison is of
  * meaning, not spelling: `\t` is a tab, `latin1` is `ISO-8859-1`, `TRUE` is `true`. */
private[invaract] object CsvOptionsChecker {

  /** Every finding for the write at the top of `plan` and the reads it makes, from the options `used` with each location. */
  def check(contract: Contract, plan: Plan, reads: List[Read], used: Map[String, Map[String, String]]): List[Violation] =
    if (used.isEmpty) Nil
    else outputFindings(contract, plan, used) ++ inputFindings(contract, reads, used)

  private def outputFindings(contract: Contract, plan: Plan, used: Map[String, Map[String, String]]): List[Violation] =
    plan match {
      case write: Write =>
        (for {
          output <- OutputChecker.expectedOutputFor(contract.outputs, OutputChecker.namesOf(write))
          options <- used.get(write.dataset.location)
          finding <- findings(output, options, SchemaChecker.Side.Output, write.dataset.location)
        } yield finding).toList
      case _ => Nil
    }

  private def inputFindings(contract: Contract, reads: List[Read], used: Map[String, Map[String, String]]): List[Violation] = {
    val declared = InputChecker.Declared(contract.inputs)
    reads.map(_.dataset.location).distinct.flatMap { location =>
      for {
        options <- used.get(location).toList
        index <- declared.index.matchingIndicesOf(LocationMatching.namesOf(location, None)).headOption.toList
        finding <- findings(declared.inputs(index), options, SchemaChecker.Side.Input, location)
      } yield finding
    }
  }

  /** The dataset's declared options against `actual`, as one violation listing every option that differs, or none. */
  def findings(dataset: Dataset, actual: Map[String, String], side: SchemaChecker.Side, location: String): Option[Violation] = {
    val differing = dataset.csv.toList.flatMap(_.declared).collect {
      case (name, expected) if actual.get(name).exists(a => !same(name, expected, a)) => (name, expected, actual(name))
    }
    if (differing.isEmpty) None
    else Some(Violations.csvOptionsMismatch(side, describe(differing.map(d => d._1 -> d._2)), describe(differing.map(d => d._1 -> d._3)), location))
  }

  /** Whether `expected` and `actual` are one value of the option `name`. */
  def same(name: String, expected: String, actual: String): Boolean =
    name match {
      case "delimiter" | "quote" | "escape" =>
        (CsvText.character(expected), CsvText.character(actual)) match {
          case (Some(e), Some(a)) => e == a
          case _                  => expected == actual
        }
      case "header"   => expected.trim.equalsIgnoreCase(actual.trim)
      case "encoding" => CsvText.canonicalEncoding(expected).equalsIgnoreCase(CsvText.canonicalEncoding(actual))
      case _          => expected == actual
    }

  private def describe(options: List[(String, String)]): String =
    options.map { case (name, value) => if (name == "header") s"$name $value" else s"$name '${printable(value)}'" }.mkString(", ")

  private def printable(value: String): String = if (value == "\t") "\\t" else value

  /** The option names a CSV dataset can declare (`CsvOptions.Names`), for adapters that report what they used. */
  val Names: List[String] = CsvOptions.Names
}
