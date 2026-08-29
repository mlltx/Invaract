# Self-contained image for running Invaract's contract regression pack
# (dev/regression) without installing anything locally. Mirrors
# .devcontainer/post-create.sh — same JDK 21 base, same Spark 3.5.1
# distribution — so behavior matches the Codespace dev environment.
#
# Build and run:
#   docker build -t invaract-regression -f docker/Dockerfile .
#   docker run --rm invaract-regression
#
# Or in one command: ./dev/regression-docker

FROM eclipse-temurin:21-jdk-jammy

RUN apt-get update && apt-get install -y --no-install-recommends \
      curl \
      ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# sbt's official "universal" distribution — a self-contained launcher that
# only needs a JVM, avoiding any ambiguity about where an installer like
# Coursier places the binary.
ENV SBT_VERSION=1.9.8
RUN curl -fsSL "https://github.com/sbt/sbt/releases/download/v${SBT_VERSION}/sbt-${SBT_VERSION}.tgz" \
      | tar xz -C /usr/local/share \
    && ln -s /usr/local/share/sbt/bin/sbt /usr/local/bin/sbt

# Apache Spark 3.5.1, for spark-submit — same distribution as
# .devcontainer/post-create.sh and .github/workflows/test.yml.
ENV SPARK_VERSION=3.5.1
ENV SPARK_HOME=/opt/spark
RUN curl -fsSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" \
      -o /tmp/spark.tgz \
    && mkdir -p "$SPARK_HOME" \
    && tar -xzf /tmp/spark.tgz -C "$SPARK_HOME" --strip-components=1 \
    && rm /tmp/spark.tgz
ENV PATH="${SPARK_HOME}/bin:${PATH}"

WORKDIR /workspace
COPY . .

# Fetch dependencies and build every module jar at image-build time, in the
# dependency order dev/build encodes, so `docker run` only has to execute
# the regression pack rather than rebuild Scala on every invocation.
RUN ./dev/build

ENV SKIP_BUILD=1
ENTRYPOINT ["./dev/regression"]
