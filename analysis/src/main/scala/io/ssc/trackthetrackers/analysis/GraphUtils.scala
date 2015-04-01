/**
 * Track the trackers
 * Copyright (C) 2014  Sebastian Schelter, Felix Neutatz
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

import org.apache.flink.api.java.CollectionEnvironment
import org.apache.flink.api.scala.{DataSet, _}

object GraphUtils {

  def readVertices(file: String)(implicit env: ExecutionEnvironment) = {
    env.readCsvFile[AnnotatedVertex](file, "\n", "\t")
  }

  def readVerticesC(file: String)(implicit env: CollectionEnvironment) = {
    env.readCsvFile(file)
  }

  def readEdges(file: String)(implicit env: ExecutionEnvironment) = {
    env.readCsvFile[Edge](file, "\n", "\t")
  }

  def toAdjacencyList(edges: DataSet[Edge]): DataSet[AdjacencyList] = {

    edges.map { edge => AdjacencyList(edge.src, Array(edge.target)) }
         .groupBy { _.src }
         .reduce { (list1, list2) => AdjacencyList(list1.src, list1.targets ++ list2.targets) }
  }

}
