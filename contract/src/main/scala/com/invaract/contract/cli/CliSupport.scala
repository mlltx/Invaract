// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.contract.cli

import java.io.File

/** Small argument-parsing/file-discovery helpers shared by every lint CLI in
  * this package (`OrgPolicyLintCli`, `CrossContractLintCli`) — previously
  * duplicated verbatim in each, which meant a fix to either (a flag-parsing
  * edge case, a `.yml`/`.yaml` walk detail) had to be remembered and
  * reapplied to the other by hand. `private[cli]`, not `private`: both
  * callers already live in this same package.
  */
private[cli] object CliSupport {

  /** Extracts `flagName VALUE` from anywhere in `args`: `Right(Some(v))`
    * when present with a following token to take as its value, `Right(None)`
    * when the flag isn't present at all, `Left(())` when it's present but is
    * the very last argument, with no value to take. The second element is
    * `args` with the flag and its value (if consumed) removed, in original
    * order. `OrgPolicyLintCli.extractIntFlag` builds on this same
    * locate/extract logic, plus an integer parse on the leaf value.
    */
  def extractStringFlag(args: Array[String], flagName: String): (Either[Unit, Option[String]], Array[String]) = {
    val idx = args.indexOf(flagName)
    if (idx < 0) (Right(None), args)
    else if (idx == args.length - 1) (Left(()), args.take(idx))
    else (Right(Some(args(idx + 1))), args.take(idx) ++ args.drop(idx + 2))
  }

  /** `target` itself if it's a single file; every `.yaml`/`.yml` file found
    * recursively if it's a directory; empty if it's neither (doesn't exist,
    * or is some other kind of filesystem entry).
    */
  def findContractFiles(target: String): List[String] = {
    val file = new File(target)
    if (file.isDirectory) {
      def walk(dir: File): List[File] =
        Option(dir.listFiles()).toList.flatten.flatMap { f =>
          if (f.isDirectory) walk(f)
          else if (f.getName.endsWith(".yaml") || f.getName.endsWith(".yml")) List(f)
          else Nil
        }
      walk(file).map(_.getPath)
    } else if (file.isFile) {
      List(file.getPath)
    } else {
      Nil
    }
  }
}
