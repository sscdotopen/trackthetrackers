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

import io.ssc.trackthetrackers.{Edge, GraphUtils}
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.core.fs.FileSystem.WriteMode

import org.apache.flink.api.scala._

object VertexCount extends App {

  count("/home/ssc/Entwicklung/projects/trackthetrackers/src/main/resources/cfindergoogle/links.tsv",
        "/tmp/flink-scala/")

  def count(linksFile: String, outputPath: String) = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val edges = env.readCsvFile[Edge](linksFile, fieldDelimiter = '\t')
    val numVertices = GraphUtils.numVertices(edges)

    numVertices.writeAsText(outputPath + "/numVertices", WriteMode.OVERWRITE)

    env.execute()
  }

}
