// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.contract.cli

import com.invaract.contract._

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

/** Standalone command line for `ContractDraftMerger`: merges the single-write drafts a dry-run
  * printed (one YAML document per write, saved to files) into one multi-output contract, with no
  * Spark involved - the same fast, authoring-time shape as the lint CLIs in this package.
  *
  * {{{
  * sbt "contract/runMain com.invaract.contract.cli.MergeDraftsCli --id customer_pipeline --version 1.0.0 drafts/"
  * sbt "contract/runMain com.invaract.contract.cli.MergeDraftsCli --output contract.yaml write1.yaml write2.yaml"
  * }}}
  *
  * Each argument is a draft file or a directory of them (every `.yaml`/`.yml` below it, in path
  * order); drafts are merged in the order given. The merged contract is printed to standard output,
  * or written to `--output`. `--id` defaults to `merged_contract` and `--version` to `0.1.0`.
  *
  * Exit codes: `0` - merged; `1` - a draft is missing or fails to parse, or the drafts cannot be
  * merged (every conflict is printed, one per line, nothing is written); `2` - usage error (no
  * drafts named, a flag with no value, a malformed `--version`).
  */
object MergeDraftsCli {
  def main(args: Array[String]): Unit = sys.exit(run(args, System.out, System.err))

  private val Usage =
    "Usage: MergeDraftsCli [--id ID] [--version MAJOR.MINOR[.PATCH]] [--output merged.yaml] <draft-file-or-directory>..."

  private val DefaultId = "merged_contract"
  private val DefaultVersion = "0.1.0"

  /** The CLI's actual logic, `sys.exit`-free so it is directly testable; `out`/`err` are injected. */
  private[cli] def run(args: Array[String], out: java.io.PrintStream, err: java.io.PrintStream): Int = {
    def flag(remaining: Array[String], name: String): Either[String, (Option[String], Array[String])] =
      CliSupport.extractStringFlag(remaining, name) match {
        case (Left(()), _)          => Left(s"$name requires a value")
        case (Right(value), rest) => Right((value, rest))
      }

    val parsed = for {
      idAndRest      <- flag(args, "--id")
      versionAndRest <- flag(idAndRest._2, "--version")
      outputAndRest  <- flag(versionAndRest._2, "--output")
    } yield (idAndRest._1.getOrElse(DefaultId), versionAndRest._1.getOrElse(DefaultVersion), outputAndRest._1, outputAndRest._2)

    parsed match {
      case Left(problem) => err.println(problem); err.println(Usage); 2
      case Right((_, _, _, targets)) if targets.isEmpty => err.println("no draft files given"); err.println(Usage); 2
      case Right((id, rawVersion, outputPath, targets)) =>
        val version =
          try Right(ContractVersion.parse(rawVersion))
          catch { case e: ContractParseException => Left(e.getMessage) }
        version match {
          case Left(problem) => err.println(problem); err.println(Usage); 2
          case Right(v)      => mergeFiles(targets.toList, id, v, outputPath, out, err)
        }
    }
  }

  private def mergeFiles(
      targets: List[String],
      id: String,
      version: ContractVersion,
      outputPath: Option[String],
      out: java.io.PrintStream,
      err: java.io.PrintStream
  ): Int = {
    val files = targets.flatMap(target => CliSupport.findContractFiles(target).sorted)
    if (files.isEmpty) {
      err.println(s"no draft files found under: ${targets.mkString(", ")}")
      return 1
    }
    val drafts =
      try files.map(ContractParser.parseFile)
      catch { case e: Exception => err.println(s"could not read a draft: ${e.getMessage}"); return 1 }

    ContractDraftMerger.merge(drafts, id, version) match {
      case Left(conflicts) =>
        conflicts.foreach {
          case MergeConflict.MalformedDraft(index, detail) => err.println(s"conflict: ${files(index)}: $detail")
          case other                                       => err.println(s"conflict: ${other.message}")
        }
        1
      case Right(merged) =>
        val yaml = ContractParser.write(merged)
        outputPath match {
          case Some(path) =>
            Files.write(Paths.get(path), yaml.getBytes(StandardCharsets.UTF_8))
            out.println(s"merged ${files.size} draft(s) into $path")
          case None => out.print(yaml)
        }
        0
    }
  }
}
