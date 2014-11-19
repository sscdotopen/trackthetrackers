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

import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala._

object FlinkUtils {

  def groupCount[T](data: DataSet[T], extractKey: (T) => Long): DataSet[(Long, Long)] = {

    data.groupBy { extractKey }
        .reduceGroup { group => countBy(extractKey, group) }
  }

  private[this] def countBy[T](extractKey: T => Long, group: Iterator[T]): (Long, Long) = {
    val key = extractKey(group.next())

    var count = 1L
    while (group.hasNext) {
      group.next()
      count += 1
    }

    key -> count
  }

  /*
  def groupCount[T, K](data: DataSet[T], extractKey: (T) => K): DataSet[(K, Long)] = {

    data.groupBy { extractKey }
        .reduceGroup { group => countBy(extractKey, group) }
  }

  private[this] def countBy[T, K](extractKey: T => K,
                                  group: Iterator[T]): (K, Long) = {
    val key = extractKey(group.next())

    var count = 1L
    while (group.hasNext) {
      group.next()
      count += 1
    }

    key -> count
  }*/

}
