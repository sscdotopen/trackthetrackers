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

import io.ssc.trackthetrackers.analysis.{FlinkUtils, Edge}
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.util.Collector

object ComponentSizeDistribution extends App {

  componentSizeDist(
    "/home/ssc/Entwicklung/projects/trackthetrackers/analysis/src/main/resources/advogato/uris.tsv",
    "/home/ssc/Entwicklung/projects/trackthetrackers/analysis/src/main/resources/advogato/links.tsv", 100,
    "/tmp/flink-scala/componentSizes/")

  case class Assignment(vertex: Long, component: Long)

  def componentSizeDist(urisFile: String, linksFile: String, maxIterations: Int, outputDir: String) = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val initialAssignments =
      env.readCsvFile[(String, Long)](urisFile, "\n", '\t')
         .map { uriWithId => Assignment(uriWithId._2, uriWithId._2) }

    val edges = env.readCsvFile[Edge](linksFile, fieldDelimiter = '\t')
                   .flatMap { edge => Array(edge, Edge(edge.target, edge.src)) }

    val assignments = initialAssignments.iterateDelta(initialAssignments, maxIterations, Array("vertex")) {
      (solutionSet, workSet) =>

        val possibleAssignments = workSet.join(edges).where("vertex").equalTo("src") { (assignment, edge) =>
          Assignment(edge.target, assignment.component)
        }

        val candidateAssignments = possibleAssignments.groupBy("vertex").min("component")

        val updatedAssignments = candidateAssignments.join(solutionSet).where("vertex").equalTo("vertex") {
          (candidateAssignment, currentAssignment, out: Collector[Assignment]) =>
            if (candidateAssignment.component < currentAssignment.component) {
              out.collect(candidateAssignment)
            }
        }

        (updatedAssignments, updatedAssignments)
    }

    val componentsWithSize = FlinkUtils.countByKey(assignments, { assignment: Assignment => assignment.component })
    val sizeWithNumComponents =
      FlinkUtils.countByKey(componentsWithSize, { componentWithSize: (Long, Long) => componentWithSize._2 })

    sizeWithNumComponents.writeAsText(outputDir, WriteMode.OVERWRITE)

    env.execute()

  }

}
