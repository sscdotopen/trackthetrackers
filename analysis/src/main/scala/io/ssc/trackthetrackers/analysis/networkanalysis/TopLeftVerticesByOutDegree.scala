package io.ssc.trackthetrackers.analysis.networkanalysis

import io.ssc.trackthetrackers.analysis.{Edge, FlinkUtils}
import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._


object TopLeftVerticesByOutDegree extends App {

  implicit val env = ExecutionEnvironment.getExecutionEnvironment


  val edges = env.readCsvFile[Edge]("/home/ssc/Entwicklung/datasets/thirdparty/thirdparty-graph.tsv", "\n", "\t")
  val domainNames = env.readCsvFile[(String, Int)]("/home/ssc/Entwicklung/datasets/thirdparty/pld-index", "\n", "\t")

  val howMany = 10000
  val threshold = 10

  val rightDegrees =
    edges.map { edge => (edge.src, 1) }
      .groupBy(0).aggregate(Aggregations.SUM, 1)

  val topRightVerticesWithDegree =
    rightDegrees.filter { vertexWithDegree => vertexWithDegree._2 > threshold }


  val topDomainsWithDegree = topRightVerticesWithDegree.joinWithHuge(domainNames).where(0).equalTo(1) {
    (vertexWithDegree: (Int, Int), vertexWithDomain: (String, Int)) =>
      vertexWithDomain._1 -> vertexWithDegree._2
  }

  val sorted = topDomainsWithDegree.collect().sortBy { case (domain, degree) => degree }.reverse.take(howMany)

  FlinkUtils.saveAsCsv(sorted, "/home/ssc/Desktop/trackthetrackers/out/pages_with_degrees_" + howMany +".tsv")
}
