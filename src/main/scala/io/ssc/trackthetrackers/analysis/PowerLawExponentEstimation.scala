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

package io.ssc.trackthetrackers.analysis

import io.ssc.trackthetrackers.Edge
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

/**
 * http://konect.uni-koblenz.de/statistics/power
 */
object PowerLawExponentEstimation extends App {

  estimatePowerLawExponent("/home/ssc/Entwicklung/projects/trackthetrackers/src/main/resources/ucidatagama/links.tsv",
    "/tmp/flink-scala/")

  def estimatePowerLawExponent(linksFile: String, outputPath: String) = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val edges = env.readCsvFile[Edge](linksFile, fieldDelimiter = '\t')

    val verticesWithDegree =
      edges.flatMap { edge => Array(edge.src -> 1, edge.target -> 1) }
           .groupBy(0)
           .reduce { (vertexWithCount1, vertexWithCount2) =>
              vertexWithCount1._1 -> (vertexWithCount1._2 + vertexWithCount2._2)
           }

    val minDegree = verticesWithDegree.map { v => println(v);
      v }.min(1).map { _._2 }

    val numVertices =
      verticesWithDegree.map { vertexWithDegree => Tuple1(vertexWithDegree._1) }
         .distinct
         .reduceGroup{ _.size }

    val estimatedExponent =
      verticesWithDegree.cross(minDegree) { (vertexWithDegree, minDegree) =>
        println("minDegree " + minDegree)
          math.log(vertexWithDegree._2.toDouble / minDegree)
        }
        .reduce { _ + _ }
        .cross(numVertices) { (sum, numVertices) =>
        println(numVertices)
        println(sum)
        1.0 + (numVertices / sum) }


//    estimatedExponent.writeAsText(outputPath + "estimatedExponent", writeMode = WriteMode.OVERWRITE)

    estimatedExponent.print()

    env.execute()
  }

}
