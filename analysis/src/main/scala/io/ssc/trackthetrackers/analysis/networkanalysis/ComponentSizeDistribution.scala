package io.ssc.trackthetrackers.analysis.networkanalysis

import io.ssc.trackthetrackers.analysis.{Edge, FlinkUtils}
import org.apache.flink.api.java.LocalEnvironment
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.configuration.{Configuration, ConfigConstants}
import org.apache.flink.util.Collector

case class Assignment(vertex: Int, component: Int)

object ComponentSizeDistribution extends App {

  //val env = ExecutionEnvironment.getExecutionEnvironment

  val conf = new Configuration()
  conf.setDouble(ConfigConstants.TASK_MANAGER_MEMORY_FRACTION_KEY, 0.9)

  val env = ExecutionEnvironment.createLocalEnvironment(conf)

  val edgeList = env.readCsvFile[Edge]("/home/ssc/Entwicklung/datasets/thirdparty/thirdparty-graph.tsv", "\n", "\t")


  val initialAssignments = edgeList.flatMap { edge => Array(Tuple1(edge.src), Tuple1(edge.target)) }
                                   .distinct
                                   .map { id => Assignment(id._1, id._1) }

  val edges = edgeList.flatMap { edge => Array(edge, Edge(edge.target, edge.src)) }

  val assignments = initialAssignments.iterateDelta(initialAssignments, 100, Array("vertex")) {
    (solutionSet, workSet) =>

      val possibleAssignments = workSet.join(edges).where("vertex").equalTo("src") { (assignment, edge: Edge) =>
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
    FlinkUtils.countByKey(componentsWithSize, { componentWithSize: (Int, Long) => componentWithSize._2.toInt })
              .map { a: (Int, Long) => a._1 -> a._2.toInt }

  FlinkUtils.saveAsCsv(sizeWithNumComponents, "/home/ssc/Desktop/trackthetrackers/out/component_sizes.tsv")

  env.execute()

}
