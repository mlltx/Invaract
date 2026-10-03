package org.apache.thrift.transport

/**
 * Compatibility shim: libthrift 0.14.0 moved `TFramedTransport` (and its nested `Factory`)
 * from `org.apache.thrift.transport` to `org.apache.thrift.transport.layered`, keeping the
 * same public API. Hive 2.3.9's metastore - which Spark 3.5's `spark-hive` still bundles -
 * references the old names (`HiveMetaStore`: `new TFramedTransport.Factory()`;
 * `HiveMetaStoreClient`: `new TFramedTransport(TTransport)`), so without a class at the old
 * name the JVM cannot link `HiveMetaStore` and `HiveConnectorSpec` aborts with
 * `NoClassDefFoundError`. Those are real-network code paths the embedded test metastore never
 * runs; they only need to exist.
 *
 * Both classes simply extend the relocated originals, so they inherit the real implementation
 * and add no behavior. Only the two constructors Hive 2.3.9 actually calls are provided (a
 * scan of every jar on the test classpath finds no other reference). Test sources only - never
 * part of a published jar.
 *
 * Why this exists, what was checked, and when it can go away (once `spark-hive` no longer
 * bundles a Hive that references the old name): docs/CVE_REMEDIATION.md 7m and the libthrift
 * comment in spark-adapter/build.sbt.
 */
class TFramedTransport(underlying: TTransport) extends layered.TFramedTransport(underlying)

object TFramedTransport {
  class Factory extends layered.TFramedTransport.Factory
}
