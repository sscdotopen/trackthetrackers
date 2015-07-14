package io.ssc.trackthetrackers.analysis.networkanalysis

import io.ssc.trackthetrackers.analysis.{Edge, FlinkUtils}
import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
//import org.apache.flink.configuration.{ConfigConstants, Configuration}

case class VertexWithDegree(vertex: Int, degree: Int)

object Assortativity extends App {

  implicit val env = ExecutionEnvironment.getExecutionEnvironment

  val edges = env.readCsvFile[Edge]("/home/ssc/Entwicklung/datasets/thirdparty/thirdparty-graph.tsv", "\n", "\t")


  val leftDegrees = edges.map { edge => (edge.src, 1) }
                         .groupBy(0).aggregate(Aggregations.SUM, 1)
                         .map { vertexWithDegree: (Int, Int) =>
                             VertexWithDegree(vertexWithDegree._1, vertexWithDegree._2)
                         }

  val rightDegrees = edges.map { edge => (edge.target, 1) }
                          .groupBy(0).aggregate(Aggregations.SUM, 1)
                          .map { vertexWithDegree: (Int, Int) =>
                              VertexWithDegree(vertexWithDegree._1, vertexWithDegree._2)
                          }

  val degreesOfSrc = edges.joinWithTiny(leftDegrees).where("src").equalTo("vertex") {
    (e: Edge, vertexWithDegree: VertexWithDegree) => (vertexWithDegree.degree, e.target)
  }

  val degreesOfSrcAndTarget = degreesOfSrc.joinWithTiny(rightDegrees).where(1).equalTo("vertex") {
    (e: (Int, Int), vertexWithDegree: VertexWithDegree) => (e._1, vertexWithDegree.degree)
  }

  val leftDegreesWithAverageNeighborDegree = degreesOfSrcAndTarget.groupBy(0).reduceGroup {
      degreeWithNeighbors: Iterator[(Int, Int)] => averageNeighborDegree(degreeWithNeighbors) }

  val rightDegreesWithAverageNeighborDegree = 
    degreesOfSrcAndTarget.map { pair: (Int, Int) => pair._2 -> pair._1 }
      .groupBy(0).reduceGroup { degreeWithNeighbors: Iterator[(Int, Int)] => 
          averageNeighborDegree(degreeWithNeighbors) 
      }

  FlinkUtils.saveAsCsv(leftDegreesWithAverageNeighborDegree,
      "/home/ssc/Desktop/trackthetrackers/out/left_assortativity.tsv")
  FlinkUtils.saveAsCsv(rightDegreesWithAverageNeighborDegree,
      "/home/ssc/Desktop/trackthetrackers/out/right_assortativity.tsv")

  env.execute()

  def averageNeighborDegree(degreeWithNeighbors: Iterator[(Int, Int)]) = {
    val first = degreeWithNeighbors.next()
    val degree = first._1
    var sum = first._2.toLong
    var count = 1

    while (degreeWithNeighbors.hasNext) {
      val nextDegree = degreeWithNeighbors.next()
      sum += nextDegree._2
      count += 1
    }

    degree -> (sum.toDouble / count)
  }


}
