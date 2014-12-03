/**
 * Track the trackers
 * Copyright (C) 2014  Sebastian Schelter
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package io.ssc.trackthetrackers.analysis.algorithms

import io.ssc.trackthetrackers.analysis.GraphUtils
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.util.Collector

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
