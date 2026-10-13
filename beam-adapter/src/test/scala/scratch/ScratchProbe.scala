package scratch

import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.{FileIO, TextIO, WriteFiles}
import org.apache.beam.sdk.io.parquet.ParquetIO
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.runners.TransformHierarchy
import org.apache.beam.sdk.values.PCollection
import scala.collection.JavaConverters._

object ScratchProbe {
  val Avro = new org.apache.avro.Schema.Parser().parse(
    """{"type":"record","name":"Orders","fields":[{"name":"id","type":"long"},{"name":"amount","type":["null","long"],"default":null}]}""")

  def main(args: Array[String]): Unit = {
    val p = Pipeline.create(PipelineOptionsFactory.create())
    val recs: PCollection[GenericRecord] = p.apply("ReadParquet", ParquetIO.read(Avro).from("in/orders/*.parquet"))
    recs.apply("WriteParquet", FileIO.write[GenericRecord]().via(ParquetIO.sink(Avro)).to("out/report").withSuffix(".parquet"))
    p.apply("Lines", TextIO.read().from("in/lines/*.txt")).apply("WriteText", TextIO.write().to("out/text"))
    p.traverseTopologically(new Pipeline.PipelineVisitor.Defaults {
      override def enterCompositeTransform(node: TransformHierarchy#Node): Pipeline.PipelineVisitor.CompositeBehavior = {
        if (!node.isRootNode && node.getFullName.count(_ == '/') == 0)
          println(s"TOP ${node.getFullName} -> ${node.getTransform.getClass.getName}")
        node.getTransform match {
          case w: WriteFiles[_, _, _] => println(s"   WriteFiles sink=${w.getSink.getClass.getName}")
          case _ =>
        }
        Pipeline.PipelineVisitor.CompositeBehavior.ENTER_TRANSFORM
      }
    })
    // FileIO.Write reflection
    val t = recs.getPipeline // unused
    p.traverseTopologically(new Pipeline.PipelineVisitor.Defaults {
      override def enterCompositeTransform(node: TransformHierarchy#Node): Pipeline.PipelineVisitor.CompositeBehavior = {
        val c = if (node.isRootNode) null else node.getTransform.getClass
        if (c != null && c.getName.endsWith("AutoValue_FileIO_Write")) {
          def m(n: String) = { val mm = c.getDeclaredMethod(n); mm.setAccessible(true); mm.invoke(node.getTransform) }
          println("FileIO outputDirectory = " + Option(m("getOutputDirectory")).map(_.toString))
          println("FileIO tempDirectory   = " + Option(m("getTempDirectory")).map(_.toString))
          val sinkFn = m("getSinkFn").asInstanceOf[org.apache.beam.sdk.transforms.Contextful[org.apache.beam.sdk.transforms.Contextful.Fn[Object, FileIO.Sink[Object]]]]
          val sink = sinkFn.getClosure.apply(null, null)
          println("FileIO sink class      = " + sink.getClass.getName)
        }
        Pipeline.PipelineVisitor.CompositeBehavior.ENTER_TRANSFORM
      }
    })
  }
}
