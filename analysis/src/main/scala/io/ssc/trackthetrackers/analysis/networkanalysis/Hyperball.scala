package io.ssc.trackthetrackers.analysis.networkanalysis

import io.ssc.trackthetrackers.analysis.GraphUtils
import io.ssc.trackthetrackers.analysis.old.algorithms.HyperLogLog
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.util.Collector

@deprecated
object Hyperball extends App {

  estimateNeighbourhoodFunction(
    "/home/ssc/Entwicklung/projects/trackthetrackers/analysis/src/main/resources/advogato/uris.tsv",
    "/home/ssc/Entwicklung/projects/trackthetrackers/analysis/src/main/resources/advogato/links.tsv", 100,
    "/tmp/flink-scala/estimatedNF/")

  case class VertexWithCounter(id: Long, counter: HyperLogLog) {
    def merge(other: VertexWithCounter) = counter.merge(other.counter)
    def count() = counter.count
  }

  def estimateNeighbourhoodFunction(urisFile: String, linksFile: String, maxIterations: Int, outputDir: String) = {

    implicit val env = ExecutionEnvironment.getExecutionEnvironment

    val initialCounters =
      GraphUtils.readVertices(urisFile)
         .map { annotatedVertex =>
           val counter = new HyperLogLog()
           counter.observe(annotatedVertex.id)
           VertexWithCounter(annotatedVertex.id, counter)
         }

    val edges = GraphUtils.readEdges(linksFile)

    val initialCandidateCounters =
      initialCounters.join(edges).where("id").equalTo("target") { (vertexWithCounter, edge) =>
        VertexWithCounter(edge.src, vertexWithCounter.counter)
      }

    val counters = initialCounters.iterateDelta(initialCandidateCounters, maxIterations, Array("id")) {
      (solutionSet, workSet) =>

       val candidateCounters =
         workSet.groupBy("id").reduceGroup { counters =>
           val accumulator = counters.next()
           while (counters.hasNext) {
             accumulator.merge(counters.next())
           }
           accumulator
         }

       val solutionSetDelta =
         solutionSet.join(candidateCounters).where("id").equalTo("id") {
           (vertexWithCounter, candidateCounter, out: Collector[VertexWithCounter]) =>
             val previousCount = vertexWithCounter.count
             vertexWithCounter.merge(candidateCounter)

             if (vertexWithCounter.count > previousCount) {
               println(vertexWithCounter.id -> vertexWithCounter.count)
               out.collect(vertexWithCounter)
             }
         }

        val nextWorkSet =
          solutionSetDelta.join(edges).where("id").equalTo("target") { (vertexWithCounter, edge) =>
            VertexWithCounter(edge.src, vertexWithCounter.counter)
          }

        (solutionSetDelta, nextWorkSet)
    }

    counters.writeAsText(outputDir, WriteMode.OVERWRITE)

    env.execute()

  }

}
