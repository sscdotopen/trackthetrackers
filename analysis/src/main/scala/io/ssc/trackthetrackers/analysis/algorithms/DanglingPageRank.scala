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
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._

@deprecated
object DanglingPageRank extends App {

  pageRank(
    "/home/ssc/Entwicklung/projects/trackthetrackers/analysis/src/main/resources/cfindergoogle/uris.tsv",
    "/home/ssc/Entwicklung/projects/trackthetrackers/analysis/src/main/resources/cfindergoogle/links.tsv",
    15763, .15, 100, 0.0001, "/tmp/flink-scala/pageRanks/")

  case class RankedVertex(id: Long, rank: Double)
  case class DanglingVertex(id: Long)

  def pageRank(urisFile: String, linksFile: String, numVertices: Long, teleportationProbability: Double,
               maxIterations: Int, epsilon: Double, outputPath: String) = {

    implicit val env = ExecutionEnvironment.getExecutionEnvironment

    val initialRanks =
        GraphUtils.readVertices(urisFile)
           .map { annotatedVertex => RankedVertex(annotatedVertex.id , 1.0 / numVertices) }

    val edges = GraphUtils.toAdjacencyList(GraphUtils.readEdges(linksFile))

    val danglingVertices =
      initialRanks.coGroup(edges).where("id").equalTo("src") {
        (rankedVertices, adjacents, out: Collector[RankedVertex]) =>
        if (adjacents.isEmpty) {
          out.collect(rankedVertices.next)
        }
      }
      .map { x => DanglingVertex(x.id) }

    val ranks = initialRanks.iterateWithTermination(maxIterations) { currentRanks =>
      val danglingRank =
        danglingVertices
          .join(currentRanks).where("id").equalTo("id") { (danglingVertex, rankedVertex) => rankedVertex.rank }
          .reduce { _ + _ }

      val newRanks =
        currentRanks.join(edges).where("id").equalTo("src") {
            (rankedVertex, adjacent, out: Collector[RankedVertex]) =>
              for (targetId <- adjacent.targets) {
                out.collect(RankedVertex(targetId, rankedVertex.rank / adjacent.targets.length))
              }
          }
        .groupBy("id")
        .aggregate(Aggregations.SUM, "rank")
        .map(new RecomputeRank(teleportationProbability, numVertices))
        .withBroadcastSet(danglingRank, "danglingRank")

      val terminated =
        currentRanks.join(newRanks).where("id").equalTo("id") {
          (currentRank, newRank) => math.abs(currentRank.rank - newRank.rank)
        }
        .reduce { _ + _ }
        .filter { _ >= 0.0001 } //TODO use epsilon here, breaks closure cleaning unfortunately

        (newRanks, terminated)
    }

    ranks.writeAsCsv(outputPath, fieldDelimiter = "\t", writeMode = WriteMode.OVERWRITE)

    env.execute()
  }

  class RecomputeRank(teleportationProbability: Double, numVertices: Long)
    extends RichMapFunction[RankedVertex, RankedVertex] {

    override def map(r: RankedVertex): RankedVertex = {

      val danglingRank = getRuntimeContext.getBroadcastVariable[Double]("danglingRank").get(0)
      val rankFromNeighbors = r.rank

      val newRank =
        (rankFromNeighbors + (danglingRank / numVertices)) * (1.0 - teleportationProbability) +
        (teleportationProbability / numVertices)

      RankedVertex(r.id, newRank)
    }
  }

}
