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

import io.ssc.trackthetrackers.analysis.{GraphUtils, Edge}
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode

/**
 * http://konect.uni-koblenz.de/statistics/power
 *
 * Be aware that this code only considers the outdegree distribution currently
 *
 */
object PowerLawExponentEstimation extends App {

  estimatePowerLawExponent("/home/ssc/Entwicklung/projects/trackthetrackers/analysis/src/main/resources/cfindergoogle/links.tsv",
    "/tmp/flink-scala/estimatedExponent")

  def estimatePowerLawExponent(linksFile: String, outputPath: String) = {

    implicit val env = ExecutionEnvironment.getExecutionEnvironment

    val edges = GraphUtils.readEdges(linksFile)

    val verticesWithDegree =
      edges.map { edge => edge.src -> 1 }
           .groupBy(0)
           .reduce { (vertexWithCount1, vertexWithCount2) =>
              vertexWithCount1._1 -> (vertexWithCount1._2 + vertexWithCount2._2)
           }

    val minDegree = verticesWithDegree.min(1).map { _._2 }

    val numVertices =
      verticesWithDegree.map { vertexWithDegree => Tuple1(vertexWithDegree._1) }
         .distinct
         .reduceGroup{ _.size }

    val estimatedExponent =
      verticesWithDegree.cross(minDegree) { (vertexWithDegree, minDegree) =>
          math.log(vertexWithDegree._2.toDouble / minDegree)
        }
        .reduce { _ + _ }
        .cross(numVertices) { (v, n) =>

          val gamma = 1.0 + (n / v)
          val sigma = math.sqrt(n) / v

          (gamma, sigma)
        }

    estimatedExponent.writeAsText(outputPath, writeMode = WriteMode.OVERWRITE)

    env.execute()
  }

}
