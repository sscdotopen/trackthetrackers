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

import io.ssc.trackthetrackers.analysis.{GraphUtils, Edge, FlinkUtils}
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.api.scala._

@deprecated
object DegreeDistribution extends App {

  fromEdges("/home/ssc/Entwicklung/projects/trackthetrackers/analysis/src/main/resources/trackinggraph-sample.tsv",
            "/tmp/flink-scala/", 59661)

  def fromEdges(edgeFile: String, outputPath: String, numVertices: Long) = {

    implicit val env = ExecutionEnvironment.getExecutionEnvironment

    val edges = GraphUtils.readEdges(edgeFile)

    val outDegreeDist = degreeDist({ _.src }, edges, numVertices)
    val inDegreeDist = degreeDist({ _.target }, edges, numVertices)

    outDegreeDist.writeAsCsv(outputPath + "/outDegreeDist/", fieldDelimiter = "\t", writeMode = WriteMode.OVERWRITE)
    inDegreeDist.writeAsCsv(outputPath + "/inDegreeDist/", fieldDelimiter = "\t", writeMode = WriteMode.OVERWRITE)

    env.execute()
  }

  private[this] def degreeDist(extract: Edge => Long, edges: DataSet[Edge], numVertices: Long) = {

    FlinkUtils.countByKey(edges, extract)
      .groupBy { _._2 }
      .reduceGroup { verticesWithDegree =>

        val degree = verticesWithDegree.next()._2

        var count = 1.0
        while (verticesWithDegree.hasNext) {
          verticesWithDegree.next()
          count += 1
        }

        degree -> (count / numVertices)
      }
  }

}
