// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.testkit

import com.invaract.verification.{ConfigSource, InvaractConf}

/** The check that every engine-neutral setting reaches an adapter: each one in `InvaractConf.All` is given a value of its
  * own, set through the engine's configuration (`ConformanceAdapter.configSource`), and must be read back. A setting added
  * to `InvaractConf` is then caught on every adapter that does not spell it, instead of being silently unreachable on one
  * engine (a platform setting it would see nothing happen).
  */
object Settings {

  /** Every neutral setting, each with a value no other has. */
  def distinct: Map[String, String] = InvaractConf.All.map(name => name -> s"invaract-setting-$name").toMap

  /** The settings in `values` that `config` does not read back as given, leaving out `inapplicable`, in `InvaractConf`'s order. */
  def unreachable(config: ConfigSource, values: Map[String, String], inapplicable: Set[String]): List[String] =
    InvaractConf.All.filterNot(inapplicable.contains).filterNot(name => values.get(name).exists(v => config.get(name).contains(v)))
}
