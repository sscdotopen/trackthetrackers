/**
 * Track the trackers
 * Copyright (C) 2015  Sebastian Schelter, Felix Neutatz
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

package io.ssc.trackthetrackers.analysis.tooling

import io.ssc.trackthetrackers.Config
import io.ssc.trackthetrackers.analysis._
import org.apache.flink.api.common.functions.RichFilterFunction
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode

import scala.collection.JavaConversions._

object PruneHyperlinkGraph extends App {

  compute(Config.get("analysis.trackingraphsample.path"), Config.get("webdatacommons.pldarcfile.unzipped"),
    Config.get("analysis.results.path") + "prunedHyperlinkGraph")

  def compute(trackingGraphFile: String, hyperlinkGraphFile: String, outputPath: String) = {

    implicit val env = ExecutionEnvironment.getExecutionEnvironment

    val hyperlinkGraphEdges = GraphUtils.readEdges(hyperlinkGraphFile)
    val trackingGraphEdges = GraphUtils.readEdges(trackingGraphFile)

    val trackingGraphVertices = trackingGraphEdges.map { edge => Tuple1(edge.target) }
                                                  .distinct

    val prunedHyperlinkGraph = hyperlinkGraphEdges.filter(new PruneToSampleVerticesFilter)
                                                   .withBroadcastSet(trackingGraphVertices, "vertices")

    prunedHyperlinkGraph.writeAsCsv(outputPath, fieldDelimiter = "\t", writeMode = WriteMode.OVERWRITE)

    env.execute()
  }
  
  class PruneToSampleVerticesFilter extends RichFilterFunction[Edge] {

    var vertices: Set[Int] = null
    
    override def open(parameters: Configuration) = {
      vertices = getRuntimeContext.getBroadcastVariable[Tuple1[Int]]("vertices").map { _._1 }
                                                                                .toSet
    }
    
    override def filter(edge: Edge): Boolean = {
      vertices.contains(edge.src) && vertices.contains(edge.target)
    }
  }

}
