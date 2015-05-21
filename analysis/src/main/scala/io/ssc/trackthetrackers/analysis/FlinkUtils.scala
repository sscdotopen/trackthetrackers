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

import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala._

object FlinkUtils {

  def countByKey[T](data: DataSet[T], extractKey: (T) => Int): DataSet[(Int, Long)] = {

    data.groupBy { extractKey }
      .reduceGroup { group => countBy(extractKey, group) }
  }

  def countByStrKey[T](data: DataSet[T], extractKey: (T) => String): DataSet[(String, Long)] = {

    data.groupBy { extractKey }
      .reduceGroup { group => countByStr(extractKey, group) }
  }


  private[this] def countByStr[T](extractKey: T => String, group: Iterator[T]): (String, Long) = {
    val key = extractKey(group.next())

    var count = 1L
    while (group.hasNext) {
      group.next()
      count += 1
    }

    key -> count
  }

  private[this] def countBy[T](extractKey: T => Int, group: Iterator[T]): (Int, Long) = {
    val key = extractKey(group.next())

    var count = 1L
    while (group.hasNext) {
      group.next()
      count += 1
    }

    key -> count
  }

}
