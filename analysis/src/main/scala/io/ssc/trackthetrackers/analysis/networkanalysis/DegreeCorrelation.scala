package io.ssc.trackthetrackers.analysis.networkanalysis

import io.ssc.trackthetrackers.analysis.{Edge, FlinkUtils}
import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

object DegreeCorrelation extends App {

  implicit val env = ExecutionEnvironment.getExecutionEnvironment
  
  val thirdPartyEdges = env.readCsvFile[Edge]("/home/ssc/Entwicklung/datasets/thirdparty/thirdparty-graph.tsv", "\n", "\t")
  val linkEdges = env.readCsvFile[Edge]("/home/ssc/Entwicklung/datasets/webdatacommons-hyperlink2012-payleveldomain/pld-arc", "\n", "\t")

  val thirdPartyGraphDegrees = 
    thirdPartyEdges.map { edge => edge.src -> 1 }
                   .groupBy(0).aggregate(Aggregations.SUM, 1)
      
  val linkGraphOutDegrees =
    linkEdges.map { edge => edge.src -> 1 }
      .groupBy(0).aggregate(Aggregations.SUM, 1)

  val linkGraphInDegrees =
    linkEdges.map { edge => edge.target -> 1 }
      .groupBy(0).aggregate(Aggregations.SUM, 1)

  val correspondingOutDegrees = thirdPartyGraphDegrees.join(linkGraphOutDegrees).where(0).equalTo(0) {
    (thirdPartyVertexWithDegree: (Int, Int), linkVertexWithDegree: (Int, Int)) =>
    thirdPartyVertexWithDegree._2 -> linkVertexWithDegree._2
  }

  val correspondingInDegrees = thirdPartyGraphDegrees.join(linkGraphInDegrees).where(0).equalTo(0) {
    (thirdPartyVertexWithDegree: (Int, Int), linkVertexWithDegree: (Int, Int)) =>
      thirdPartyVertexWithDegree._2 -> linkVertexWithDegree._2
  }

  FlinkUtils.saveAsCsv(correspondingOutDegrees, "/home/ssc/Desktop/trackthetrackers/out/corresponding_out_degrees.tsv")
  FlinkUtils.saveAsCsv(correspondingInDegrees, "/home/ssc/Desktop/trackthetrackers/out/corresponding_in_degrees.tsv")

  env.execute()
}
