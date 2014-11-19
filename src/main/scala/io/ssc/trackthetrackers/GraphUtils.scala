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

package io.ssc.trackthetrackers

import org.apache.flink.api.scala.{DataSet, _}

object GraphUtils {

  //TODO should return a long...
  def numVertices(edges: DataSet[Edge]): DataSet[Int] = {

    val srcVertices = edges map { edge => Tuple1(edge.src) }
    val targetVertices = edges map { edge => Tuple1(edge.target) }

    (srcVertices union targetVertices).distinct.reduceGroup { _.size }
  }

  def toAdjacencyList(edges: DataSet[Edge]): DataSet[AdjacencyList] = {

    edges.map { edge => AdjacencyList(edge.src, Array(edge.target)) }
         .groupBy { _.src }
         .reduce { (list1, list2) => AdjacencyList(list1.src, list1.targets ++ list2.targets) }
  }

}
