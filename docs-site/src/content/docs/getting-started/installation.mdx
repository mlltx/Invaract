---
title: Installation
description: Prerequisites and setup for working with Invariant, in Codespaces or locally.
sidebar:
  order: 1
---

import { Tabs, TabItem, Aside, Steps } from '@astrojs/starlight/components';

<Aside type="note" title="No published package yet">
Invariant is early-stage and hasn't published a Maven artifact yet. Today, you use it by
cloning the repository and building its modules from source — either in GitHub Codespaces
(recommended, zero local setup) or on your own machine. Both paths are covered below.
</Aside>

## Prerequisites

| Tool | Version | Needed for |
|---|---|---|
| JDK | 21 | Building and running every module |
| sbt | 1.9.8 (`contract`/`plugin`/`runner`), 1.11.7 (`ir`/`spark-adapter`) | Building each module |
| Apache Spark | 3.5.1 | Running a real job via `spark-submit` |
| Node.js | 20 | The results web UI (`web/`) |

You don't need to install these by hand if you use Codespaces — its dev container
provisions all of them automatically.

## Set up

<Tabs>
<TabItem label="GitHub Codespaces">
<Steps>

1. Clone the repository and open it in GitHub Codespaces:

   ```bash
   git clone https://github.com/mlltx/Invariant.git
   ```

   Open the repository on GitHub and choose **Code → Codespaces → Create codespace**.

2. Wait for the dev container to provision. `.devcontainer/post-create.sh` installs sbt,
   Apache Spark 3.5.1, and sets `SPARK_HOME` — about 5 minutes on first boot, and nothing
   further to configure.

3. Confirm the toolchain is ready:

   ```bash
   java -version
   sbt --version
   spark-submit --version
   ```

</Steps>
</TabItem>
<TabItem label="Local machine">
<Steps>

1. Install JDK 21, then verify:

   ```bash
   java -version
   ```

2. Install sbt via [Coursier](https://get-coursier.io/):

   ```bash
   curl -fsSL https://github.com/coursier/coursier/releases/download/v2.1.9/cs-x86_64-pc-linux.gz \
     | gzip -d > cs
   chmod +x cs
   ./cs install sbt
   ```

3. Install Apache Spark 3.5.1 and put `spark-submit` on your `PATH`:

   ```bash
   SPARK_VERSION="3.5.1"
   HADOOP_VERSION="3"
   wget "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"
   tar -xzf "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" -C /opt
   mv "/opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}" /opt/spark
   export SPARK_HOME="/opt/spark"
   export PATH="$SPARK_HOME/bin:$PATH"
   ```

4. Install Node.js 20 (only needed for the results web UI).

5. Clone the repository:

   ```bash
   git clone https://github.com/mlltx/Invariant.git
   cd Invariant
   ```

</Steps>

<Aside type="caution" title="JDK 17+ note">
Spark reflectively accesses JDK-internal classes that JDK 17+'s module system closes by
default. Running via `spark-submit` (the primary path used throughout these docs) already
sets the required `--add-opens` flags for you. If you invoke a module's tests directly
with `sbt test` and hit an `InaccessibleObjectException`, see
[Troubleshooting](/troubleshooting/common-problems/).
</Aside>
</TabItem>
</Tabs>

## Next step

Continue to [Quick Start](/getting-started/quick-start/) to build every module
and run a real, contract-verified Spark job.
